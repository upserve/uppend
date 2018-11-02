package com.upserve.uppend.blobs;

import com.google.common.collect.ImmutableList;
import com.upserve.uppend.util.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.lang.StrictMath.min;

/**
 * Simulates contiguous pages for virtual files in a single physical file
 * Pages are double linked with head and tail pointers for each virtual file
 * The header maintains a table of pages for each virtual file.
 * <p>
 * Self Describing Header: virtualFiles (int), pageSize (int)
 * <p>
 * Header:
 * (long int)
 * VF1  currentPosition, pageCount
 * VF2  currentPosition, pageCount
 * VF3  currentPosition, pageCount
 * <p>
 * PageStart Table (long):
 * VF1, VF2, VF3, VF4,... VIRTUAL_FILES
 * Page1    .......            ..................
 * Page2    .......            ..................
 * Page3    .......            ..................
 * Page4    .......            ..................
 * Page5    .......            ..................
 * ...      .......            ..................
 * PAGES_PER_VIRUAL_FILE
 * <p>
 * Pages:
 * previousPageStart(long), pageSize(bytes), nextPageStart(long)
 * <p>
 * A fixed number of pages per virtual file are allocated at startup - exceeding this number would be... bad
 * TODO - fix this!
 */
public class VirtualPageFile implements Closeable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Supplier<ByteBuffer> LOCAL_INT_BUFFER = ThreadLocalByteBuffers.LOCAL_INT_BUFFER;
    private static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER;

    private static final int SELF_DESCRIBING_HEADER_SIZE = 8;

    private static final int HEADER_RECORD_SIZE = 8 + 4;
    /* firstPageStart, lastPageStart, currentPosition, pageCount */

    // Maximum number of pages allowed per virtual file
    private static final int PAGES_PER_VIRUAL_FILE = 1000;

    private static final int MAX_BUFFERS = 1024 * 64; // 128 TB per partition.
    private final MappedByteBuffer[] mappedByteBuffers;
    private final int bufferSize;

    private final Path filePath;
    private final FileChannel channel;

    private final MappedByteBuffer headerBuffer;
    private final MappedByteBuffer pageTableBuffer;

    private final AtomicLong nextPagePosition;
    private final boolean readOnly;

    private final AtomicLong[] virtualFilePositions; // the current position in the virtual file for each virtual file
    private final AtomicInteger[] virtualFilePageCounts; // the number of pages currently allocated for each virtual file

    private final LongAdder pageAllocationCount;

    private final LongBuffer pageTable; // Indexable list of page start locations for each virtual file

    private final int virtualFiles;
    private final int pageSize;

    private final int headerSize;
    private final int tableSize;

    private final int totalHeaderSize;

    private final FileChannel.MapMode mapMode;

    public Path getFilePath() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        if (!channel.isOpen()) return;
        Arrays.fill(mappedByteBuffers, null);

        if (!readOnly) {
            channel.truncate(nextPagePosition.get());
        }
        channel.close();
    }

    int getVirtualFiles() {
        return virtualFiles;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getAllocatedPageCount() {
        return pageAllocationCount.intValue();
    }

    // Package private methods
    boolean isPageAvailable(int virtualFileNumber, int pageNumber) {
        if (readOnly) {
            return pageNumber < getHeaderVirtualFilePageCount(virtualFileNumber);
        } else {
            return pageNumber < virtualFilePageCounts[virtualFileNumber].get();
        }
    }

    long appendPosition(int virtualFileNumber, int size) {
        // return the position to write at
        final long result = getAtomicVirtualFilePosition(virtualFileNumber).getAndAdd(size);
        // record the position written too
        putHeaderVirtualFilePosition(virtualFileNumber, result + size);
        // It is possible to have a race here which could result in loosing an appended value if the writer process dies
        // before it writes again...
        return result;
    }

    long appendPageAlignedPosition(int virtualFileNumber, int size, int lowBound, int highBound) {

        long[] effectivelyFinal = new long[1];
        getAtomicVirtualFilePosition(virtualFileNumber).getAndUpdate(val -> {

            int naturalPageStartPosition = pagePosition(val);
            int availableSpace = pageSize - naturalPageStartPosition;

            if (availableSpace >= highBound) {
                effectivelyFinal[0] = val;
                return val + size;
            } else if (availableSpace <= lowBound) {
                effectivelyFinal[0] = val;
                return val + size;
            } else {
                effectivelyFinal[0] = val + availableSpace - lowBound;
                return val + size + availableSpace - lowBound;
            }
        });

        final long result = effectivelyFinal[0];
        putHeaderVirtualFilePosition(virtualFileNumber, result + size);
        return result;
    }

    long nextAlignedPosition(long position, int lowBound, int highBound) {
        int naturalPageStartPosition = pagePosition(position);
        int availableSpace = pageSize - naturalPageStartPosition;

        if (availableSpace >= highBound) {
            return position;
        } else if (availableSpace <= lowBound) {
            return position;
        } else {
            return position + availableSpace - lowBound;
        }
    }

    long getPosition(int virtualFileNumber) {
        if (readOnly) {
            return getHeaderVirtualFilePosition(virtualFileNumber);
        } else {
            return virtualFilePositions[virtualFileNumber].get();
        }
    }

    /**
     * Get the position in the page for the virtual file position
     *
     * @param pos the position in the virtual file
     * @return the position in the page
     */
    int pagePosition(long pos) {
        return (int) (pos % (long) pageSize);
    }

    /**
     * Get the page number in the virtual file for a given position
     *
     * @param pos the position in the virtual file
     * @return the page this position occurs in
     */
    int pageNumber(long pos) {
        long result = (pos / (long) pageSize);
        if (result >= PAGES_PER_VIRUAL_FILE)
            throw new IllegalStateException("The position " + pos + " exceeds the page limit for file" + getFilePath());
        return (int) result;
    }

    /**
     * Get or create (allocate) the page if it does not exist.
     *
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    Page getOrCreatePage(int virtualFileNumber, int pageNumber) {
        final long startPosition;
        if (isPageAvailable(virtualFileNumber, pageNumber)) {
            startPosition = getValidPageStart(virtualFileNumber, pageNumber);
        } else {
            startPosition = allocatePosition(virtualFileNumber, pageNumber);
        }

       return filePage(startPosition);
    }

    /**
     * Get the existing page
     *
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    Page getExistingPage(int virtualFileNumber, int pageNumber) {
        long startPosition = getValidPageStart(virtualFileNumber, pageNumber);
        return mappedPage(startPosition);
    }

    MappedPage mappedPage(long startPosition) {
        final long postHeaderPosition = startPosition - (totalHeaderSize);
        final int mapIndex = (int) (postHeaderPosition / bufferSize);
        final int mapPosition = (int) (postHeaderPosition % bufferSize);

        MappedByteBuffer bigbuffer = ensureBuffered(mapIndex);

        return new MappedPage(bigbuffer, mapPosition, pageSize);
    }

    FilePage filePage(long startPosition) {
        return new FilePage(channel, startPosition, pageSize);
    }

    long getFileSize(){
        try {
            return channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not get file size for:" + filePath, e);
        }
    }

    public VirtualPageFile(Path filePath, int virtualFiles, int pageSize, int targetBufferSize, boolean readOnly) {
        this.filePath = filePath;
        this.readOnly = readOnly;
        this.virtualFiles = virtualFiles;
        this.pageSize = pageSize;

        this.mappedByteBuffers = new MappedByteBuffer[MAX_BUFFERS];

        if (targetBufferSize < (pageSize)) throw new IllegalArgumentException("Target buffer size " + targetBufferSize + " must be larger than a page " + pageSize);

        this.bufferSize = (targetBufferSize / (pageSize)) * (pageSize);

        log.debug("Using buffer size " + bufferSize + " with page size " + pageSize);

        if (virtualFiles < 1) throw new IllegalArgumentException("virtualFiles must be greater than 0 in file: " + filePath);

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
            mapMode = FileChannel.MapMode.READ_ONLY;
        } else {
            openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
            mapMode = FileChannel.MapMode.READ_WRITE;
        }

        try {
            channel = FileChannel.open(filePath, openOptions);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to open file: " + filePath, e);
        }

        final long initialSize;
        try {
            initialSize = channel.size();
            ByteBuffer intBuffer = LOCAL_INT_BUFFER.get();
            if (!readOnly && initialSize == 0) {
                intBuffer.putInt(virtualFiles);
                channel.write(intBuffer.flip(), 0);

                intBuffer.flip().putInt(pageSize);
                channel.write(intBuffer.flip(), 4);
            } else {
                channel.read(intBuffer, 0);
                int val = intBuffer.flip().getInt();
                if (val != virtualFiles)
                    throw new IllegalArgumentException("The specfied number of virtual files " + virtualFiles + " does not match the value in the datastore " + val + " in file " + getFilePath());

                channel.read(intBuffer, 4);
                val = intBuffer.flip().getInt();
                if (val != virtualFiles)
                    throw new IllegalArgumentException("The specfied page size " + pageSize + " does not match the value in the datastore " + val + " in file " + getFilePath());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read, write or get the size of " + getFilePath(), e);
        }

        headerSize = virtualFiles * HEADER_RECORD_SIZE;
        tableSize = virtualFiles * PAGES_PER_VIRUAL_FILE * 8;

        totalHeaderSize = headerSize + tableSize + SELF_DESCRIBING_HEADER_SIZE;

        try {
            headerBuffer = channel.map(mapMode, SELF_DESCRIBING_HEADER_SIZE, headerSize);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map header for path: " + filePath, e);
        }

        virtualFilePositions = IntStream
                .range(0, virtualFiles)
                .mapToLong(this::getHeaderVirtualFilePosition)
                .mapToObj(AtomicLong::new)
                .toArray(AtomicLong[]::new);

        virtualFilePageCounts = IntStream
                .range(0, virtualFiles)
                .map(this::getHeaderVirtualFilePageCount)
                .mapToObj(AtomicInteger::new)
                .toArray(AtomicInteger[]::new);

        // For statistics...
        pageAllocationCount = new LongAdder();
        pageAllocationCount.add(Arrays.stream(virtualFilePageCounts).mapToLong(AtomicInteger::get).sum());

        try {
            pageTableBuffer = channel.map(mapMode, headerSize + SELF_DESCRIBING_HEADER_SIZE, tableSize);
            pageTable = pageTableBuffer.asLongBuffer();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map page locations for path: " + filePath, e);
        }


        long lastStartPosition = IntStream
                .range(0, virtualFiles)
                .mapToLong(index -> {
                    int pageCount = virtualFilePageCounts[index].get();
                    int pageIndex = pageCount > 0 ? pageCount - 1 : 0;
                    return getRawPageStart(index,  pageIndex);
                })
                .max().orElse(0L);

        if (lastStartPosition == 0) {
            nextPagePosition = new AtomicLong(totalHeaderSize);
        } else if (lastStartPosition < totalHeaderSize) {
            throw new IllegalStateException("file position " + lastStartPosition + " is less than header size: " + headerSize + " in file " + filePath);
        } else {
            nextPagePosition = new AtomicLong(lastStartPosition + pageSize);
            preloadBuffers(lastStartPosition + pageSize);
        }
    }

    private long getRawPageStart(int virtualFileNumber, int pageNumber) {
        return pageTable.get(PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber);
    }

    private long getValidPageStart(int virtualFileNumber, int pageNumber) {
        if (pageNumber == -1) return -1L;
        long result = getRawPageStart(virtualFileNumber, pageNumber);
        if (result < totalHeaderSize) {
            throw new IllegalStateException("Invalid page position " + result + " is in the file header; in page table for file " + virtualFileNumber + " page " + pageNumber + " in file " + getFilePath());
        }
        if ((result - (totalHeaderSize)) % (pageSize) != 0 ) {
            throw new IllegalStateException("Invalid page position " + result + " is not aligned with pageSize " + pageSize + "; in page table for file " + virtualFileNumber + " page " + pageNumber + " in file " + getFilePath());
        }
        return result;
    }

    private void putPageStart(int virtualFileNumber, int pageNumber, long position) {
        int index = PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber;
        pageTable.put(index, position);
    }

    private long getHeaderVirtualFilePosition(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE);
    }

    private void putHeaderVirtualFilePosition(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE, position);
    }

    private AtomicLong getAtomicVirtualFilePosition(int virtualFileNumber) {
        return virtualFilePositions[virtualFileNumber];
    }

    private int getHeaderVirtualFilePageCount(int virtualFileNumber) {
        return headerBuffer.getInt(virtualFileNumber * HEADER_RECORD_SIZE + 8);
    }

    private void putHeaderVirtualFilePageCount(int virtualFileNumber, int count) {
        headerBuffer.putInt(virtualFileNumber * HEADER_RECORD_SIZE + 8, count);
    }

    private long allocatePosition(int virtualFileNumber, int pageNumber) {
        synchronized (virtualFilePageCounts[virtualFileNumber]) {
            // If another thread has already done it - just return the start position
            final int currentPageCount = virtualFilePageCounts[virtualFileNumber].get();
            if (pageNumber >= currentPageCount) {
                allocatePage(virtualFileNumber, currentPageCount, pageNumber);
            }
        }
        return getValidPageStart(virtualFileNumber, pageNumber);
    }

    private void allocatePage(int virtualFileNumber, int currentPageCount, int pageNumber) {

        int pagesToAllocate = pageNumber - currentPageCount + 1;
        // Do the atomic stuff
        long firstPageStart = nextPagePosition.getAndAdd(pageSize * pagesToAllocate);

        for (int i=0; i < pagesToAllocate; i++) {
            // Update the persistent table of pages
            putPageStart(virtualFileNumber, currentPageCount + i, firstPageStart + i * pageSize);

            // Now that the page is allocated and persistent - update the counter which is the lock controlling access
        }

        putHeaderVirtualFilePageCount(virtualFileNumber, currentPageCount + pagesToAllocate);
        virtualFilePageCounts[virtualFileNumber].set(currentPageCount + pagesToAllocate);

        // Stats only
        pageAllocationCount.add(pagesToAllocate);
    }


    private MappedByteBuffer ensureBuffered(int bufferIndex) {
        MappedByteBuffer buffer = mappedByteBuffers[bufferIndex];
        if (buffer == null) {
            synchronized (mappedByteBuffers) {
                buffer = mappedByteBuffers[bufferIndex];
                if (buffer == null) {
                    long bufferStart = ((long) bufferIndex * bufferSize) + totalHeaderSize;
                    try {
                        buffer = channel.map(FileChannel.MapMode.READ_ONLY, bufferStart, bufferSize);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unable to map buffer for index " + bufferIndex + " at (" + bufferStart +  " start position) in file " + filePath, e);
                    }
                    mappedByteBuffers[bufferIndex] = buffer;
                }
            }
        }
        return buffer;
    }

    // Called during initialize only - no need to synchronize
    private void preloadBuffers(long nextPagePosition){
        for (int bufferIndex=0; bufferIndex<MAX_BUFFERS; bufferIndex++){
            long bufferStart = ((long) bufferIndex * bufferSize) + totalHeaderSize;

            if (bufferStart >= nextPagePosition) break;

            try {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, bufferStart, bufferSize);
                mappedByteBuffers[bufferIndex] = buffer;
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to preload mapped buffer for index " + bufferIndex + " at (" + bufferStart + " start position) in file "  + filePath, e);
            }
        }
    }
}
