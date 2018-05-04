package com.upserve.uppend.blobs;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Simulates contiguous pages for virtual files in a single physical file
 * Pages are double linked with head and tail pointers for each virtual file
 * The header maintains a table of pages for each virtual file.
 * <p>
 * Self Describing Header: virtualFiles (int), pageSize (int)
 * <p>
 * Header:
 * (long, long, long int)
 * VF1  firstPageStart, lastPageStart, currentPosition, pageCount
 * VF2  firstPageStart, lastPageStart, currentPosition, pageCount
 * VF3  firstPageStart, lastPageStart, currentPosition, pageCount
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
 * <p>
 * TODO put the number of virtual files and the page size in the file header and check on opening
 */
public class VirtualPageFile implements Flushable, Closeable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER;
    private static final Supplier<ByteBuffer> LOCAL_INT_BUFFER = ThreadLocalByteBuffers.LOCAL_INT_BUFFER;
    private static final int SELF_DESCRIBING_HEADER_SIZE = 8;

    private static final int HEADER_RECORD_SIZE = 8 + 8 + 8 + 4;
    /* firstPageStart, lastPageStart, currentPosition, pageCount */

    // Maximum number of pages allowed per virtual file
    private static final int PAGES_PER_VIRUAL_FILE = 1000;

    private final Path filePath;
    private final FileChannel channel;

    private final MappedByteBuffer headerBuffer;
    private final MappedByteBuffer pageTableBuffer;

    private final AtomicLong nextPagePosition;
    private final boolean readOnly;

    private final AtomicLong[] lastPagePositions; // the position in the physical file of the last page for each virtual file
    private final AtomicLong[] firstPagePositions; // the position in the physical file of first page for each virtual file
    private final AtomicLong[] virtualFilePositions; // the current position in the virtual file for each virtual file
    private final AtomicInteger[] virtualFilePageCounts; // the number of pages currently allocated for each virtual file

    private final LongBuffer pageTable; // Indexable list of page start locations for each virtual file

    private final int virtualFiles;
    private final int pageSize;

    private final int headerSize;
    private final int tableSize;

    private final PageCache pageCache;
    private final FileChannel.MapMode mapMode;

    // Public methods
    public VirtualPageFile(Path filePath, int virtualFiles, int pageSize, boolean readOnly) {
        this(filePath, virtualFiles, pageSize, readOnly, null);
    }

    public VirtualPageFile(Path filePath, int virtualFiles, boolean readOnly, PageCache pageCache) {
        this(filePath, virtualFiles, pageCache.getPageSize(), readOnly, pageCache);
    }


    public Path getFilePath() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }

    @Override
    public void flush() throws IOException {
        headerBuffer.force();
        pageTableBuffer.force();
        channel.force(true);
    }

    public int getVirtualFiles() {
        return virtualFiles;
    }

    public boolean isReadOnly() {
        return readOnly;
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
            throw new IllegalStateException("The position " + pos + " exceeds the page limit for this file");
        return (int) result;
    }

    /**
     * Get or create (allocate) the page if it does not exist.
     * Returns a MappedByteBuffer backed Page if there is one in the cache. Otherwise it returns a FilePage
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

        if (pageCache != null) {
            return pageCache.getIfPresent(this, startPosition).orElse(filePage(startPosition));
        } else {
            return filePage(startPosition);
        }
    }

    /**
     * Get a MappedByteBuffer backed Page uses a page cache if present
     *
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    Page getExistingPage(int virtualFileNumber, int pageNumber) {
        // TODO we could cache each page start in the writer if reading the long from the mapped byte buffer gets expensive
        long startPosition = getValidPageStart(virtualFileNumber, pageNumber);

        if (pageCache != null) {
            return pageCache.get(startPosition, getFilePath(), pageKey -> mappedPage(pageKey.getPosition()));
        } else {
            return mappedPage(startPosition);
        }
    }

    MappedPage mappedPage(long startPosition) {
        try {
            return new MappedPage(channel.map(mapMode, startPosition + 8, pageSize));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to map page from file " + filePath, e);
        }
    }

    FilePage filePage(long startPosition) {
        return new FilePage(channel, startPosition + 8, pageSize);
    }

    // Private methods
    private VirtualPageFile(Path filePath, int virtualFiles, int pageSize, boolean readOnly, PageCache pageCache) {
        this.filePath = filePath;
        this.readOnly = readOnly;
        this.virtualFiles = virtualFiles;
        this.pageSize = pageSize;
        this.pageCache = pageCache;

        if (virtualFiles < 1) throw new IllegalArgumentException("virtualFiles must be greater than 0");

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ};
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


        try {
            long initialSize = channel.size();
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
                    throw new IllegalArgumentException("The specfied number of virtual files " + virtualFiles + " does not match the value in the datastore " + val);

                channel.read(intBuffer, 4);
                val = intBuffer.flip().getInt();
                if (val != virtualFiles)
                    throw new IllegalArgumentException("The specfied page size " + pageSize + " does not match the value in the datastore " + val);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get file size", e);
        }


        headerSize = virtualFiles * HEADER_RECORD_SIZE;
        tableSize = virtualFiles * PAGES_PER_VIRUAL_FILE * 8;

        try {
            headerBuffer = channel.map(mapMode, SELF_DESCRIBING_HEADER_SIZE, headerSize);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map header for path: " + filePath, e);
        }

        firstPagePositions = IntStream
                .range(0, virtualFiles)
                .mapToLong(this::getHeaderFirstPage)
                .mapToObj(AtomicLong::new)
                .toArray(AtomicLong[]::new);

        lastPagePositions = IntStream
                .range(0, virtualFiles)
                .mapToLong(this::getHeaderLastPage)
                .mapToObj(AtomicLong::new)
                .toArray(AtomicLong[]::new);

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

        long lastStartPosition = Arrays.stream(lastPagePositions).mapToLong(AtomicLong::get).max().orElse(0L);

        try {
            pageTableBuffer = channel.map(mapMode, headerSize + SELF_DESCRIBING_HEADER_SIZE, tableSize);
            pageTable = pageTableBuffer.asLongBuffer();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map page locations for path: " + filePath, e);
        }

        if (lastStartPosition == 0) {
            nextPagePosition = new AtomicLong(headerSize + tableSize + SELF_DESCRIBING_HEADER_SIZE);

        } else if (lastStartPosition < headerSize + tableSize + SELF_DESCRIBING_HEADER_SIZE) {
            throw new IllegalStateException("file position " + lastStartPosition + " is less than header size: " + headerSize + " in file " + filePath);
        } else {
            nextPagePosition = new AtomicLong(lastStartPosition + pageSize + 16);
        }

        IntStream.range(0, virtualFiles).parallel().forEach(this::detectCorruption);

        // TODO Can we fix corruption instead of just bailing?
    }

    private void detectCorruption(int virtualFileNumber) {

        long virtualFilePosition = virtualFilePositions[virtualFileNumber].get();
        int pageCount = virtualFilePageCounts[virtualFileNumber].get();
        long firstPageStart = firstPagePositions[virtualFileNumber].get();
        long finalPageStart = lastPagePositions[virtualFileNumber].get();

        long[] pageStarts = IntStream.range(0, PAGES_PER_VIRUAL_FILE).mapToLong(page -> getRawPageStart(virtualFileNumber, page)).toArray();

        if (pageCount == 0) {
            if (virtualFilePosition != 0 || firstPageStart != 0 || finalPageStart != 0 || Arrays.stream(pageStarts).anyMatch(val -> val != 0)) {
                throw new IllegalStateException("None zero positions for file with no pages!");
            }
            return;
        }

        if (virtualFilePosition / pageSize > pageCount)
            throw new IllegalStateException("The current virtual file position is outside the last page!");

        if (firstPageStart != pageStarts[0])
            throw new IllegalStateException("Header first pageStart does not match table page 0 start");
        if (finalPageStart != pageStarts[pageCount - 1])
            throw new IllegalStateException("Header last pageStart does not match table page last start");

        long nextPageStart = firstPageStart;
        long lastPageStart = -1L;
        for (int page = 0; page < pageCount; page++) {
            if (nextPageStart != pageStarts[page])
                throw new IllegalStateException("Head pointer does not match table page start");
            if (readTailPointer(nextPageStart) != lastPageStart)
                throw new IllegalStateException("Corrupt tail pointer in first page");

            lastPageStart = nextPageStart;

            nextPageStart = readHeadPointer(nextPageStart);
        }

        if (nextPageStart != -1) throw new IllegalStateException("Last head pointer not equal -1");
    }

    private long getRawPageStart(int virtualFileNumber, int pageNumber) {
        return pageTable.get(PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber);
    }

    private long getValidPageStart(int virtualFileNumber, int pageNumber) {
        if (pageNumber == -1) return -1L;
        long result = getRawPageStart(virtualFileNumber, pageNumber);
        if (result < headerSize + tableSize) {
            throw new IllegalStateException("Invalid page position in page table for file " + virtualFileNumber + " page " + pageNumber);
        }
        return result;
    }

    private void putPageStart(int virtualFileNumber, int pageNumber, long position) {
        int index = PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber;
        pageTable.put(index, position);
    }

    private long getHeaderFirstPage(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE);
    }

    private void putHeaderFirstPage(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE, position);
    }

    private long getHeaderLastPage(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE + 8);
    }

    private void putHeaderLastPage(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE + 8, position);
    }

    private long getHeaderVirtualFilePosition(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE + 16);
    }

    private void putHeaderVirtualFilePosition(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE + 16, position);
    }

    private AtomicLong getAtomicVirtualFilePosition(int virtualFileNumber) {
        return virtualFilePositions[virtualFileNumber];
    }

    private int getHeaderVirtualFilePageCount(int virtualFileNumber) {
        return headerBuffer.getInt(virtualFileNumber * HEADER_RECORD_SIZE + 24);
    }

    private void putHeaderVirtualFilePageCount(int virtualFileNumber, int count) {
        headerBuffer.putInt(virtualFileNumber * HEADER_RECORD_SIZE + 24, count);
    }

    private long allocatePosition(int virtualFileNumber, int pageNumber) {
        synchronized (virtualFilePageCounts[virtualFileNumber]) {
            // If another thread has already done it - just return the start position
            while (pageNumber >= virtualFilePageCounts[virtualFileNumber].get()) {
                allocatePage(virtualFileNumber);
            }
        }
        return getValidPageStart(virtualFileNumber, pageNumber);
    }

    private void allocatePage(int virtualFileNumber) {

        // Do the atomic stuff
        long newPageStart = nextPagePosition.getAndAdd(pageSize + 16);
        int newPageNumber = virtualFilePageCounts[virtualFileNumber].get(); // the result is the index, the value incremented at the end of the method is the new count!
        boolean firstPage = firstPagePositions[virtualFileNumber].compareAndSet(0L, newPageStart);
        lastPagePositions[virtualFileNumber].set(newPageStart);

        long lastPageStart = getValidPageStart(virtualFileNumber, newPageNumber - 1);

        if (lastPageStart > 0) writeHeadPointer(lastPageStart, newPageStart);
        writeTailPointer(newPageStart, lastPageStart);
        writeHeadPointer(newPageStart, -1); // Extends the file to the end of the page

        // Update the persistent table of pages
        putPageStart(virtualFileNumber, newPageNumber, newPageStart);

        // Update the persisted header values
        if (firstPage) putHeaderFirstPage(virtualFileNumber, newPageStart);
        putHeaderLastPage(virtualFileNumber, newPageStart);
        putHeaderVirtualFilePageCount(virtualFileNumber, newPageNumber + 1);

        // Now that the page is allocated and persistent - update the counter which is the lock controlling access
        virtualFilePageCounts[virtualFileNumber].getAndIncrement();
    }

    private void writeTailPointer(long pageStart, long previousPageStart) {
        ByteBuffer longBuffer = LOCAL_LONG_BUFFER.get();
        longBuffer.asLongBuffer().put(previousPageStart); // will be -1 if this is the first page
        try {
            channel.write(longBuffer, pageStart);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write tail pointer at " + pageStart + " in " + filePath, e);
        }
    }

    private long readTailPointer(long pageStart) {
        ByteBuffer longBuffer = LOCAL_LONG_BUFFER.get();

        try {
            channel.read(longBuffer, pageStart);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read tail pointer at " + pageStart + " in " + filePath, e);
        }
        return longBuffer.flip().getLong();
    }

    private void writeHeadPointer(long pageStart, long nextPageStart) {
        ByteBuffer longBuffer = LOCAL_LONG_BUFFER.get();
        longBuffer.asLongBuffer().put(nextPageStart); // will be -1 if this is the first page
        try {
            channel.write(longBuffer, pageStart + pageSize + 8);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write head pointer at " + pageStart + " in " + filePath, e);
        }
    }

    private long readHeadPointer(long pageStart) {
        ByteBuffer longBuffer = LOCAL_LONG_BUFFER.get();

        try {
            channel.read(longBuffer, pageStart + pageSize + 8);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read head pointer at " + pageStart + " in " + filePath, e);
        }
        return longBuffer.flip().getLong();
    }
}
