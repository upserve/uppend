package com.upserve.uppend.blobs;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
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

/**
 * Simulates contiguous pages for virtual files in a single physical file
 * Pages are double linked with head and tail pointers for each virtual file
 * The header maintains a table of pages for each virtual file.
 *
 * Header:
 *      (long, long, long int)
 * VF1  firstPageStart, lastPageStart, currentPosition, pageCount
 * VF2  firstPageStart, lastPageStart, currentPosition, pageCount
 * VF3  firstPageStart, lastPageStart, currentPosition, pageCount
 *
 * PageStart Table (long):
 *          VF1, VF2, VF3, VF4,... VIRTUAL_FILES
 * Page1    .......            ..................
 * Page2    .......            ..................
 * Page3    .......            ..................
 * Page4    .......            ..................
 * Page5    .......            ..................
 * ...      .......            ..................
 * PAGES_PER_VIRUAL_FILE
 *
 * Pages:
 * previousPageStart(long), pageSize(bytes), nextPageStart(long)
 *
 * A fixed number of pages per virtual file are allocated at startup - exceeding this number would be... bad
 * TODO - fix this!
 *
 */
public class VirtualPageFile implements Flushable, Closeable{
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER;

    private static final int HEADER_RECORD_SIZE =  8 + 8 + 8 + 4;
    private static final int PAGES_PER_VIRUAL_FILE = 100;

    /* firstPageStart, currentPosition, pageCount */

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

    public VirtualPageFile(Path filePath, int virtualFiles, int pageSize, boolean readOnly) {
        this(filePath, virtualFiles, pageSize, readOnly, null);
    }

    public VirtualPageFile(Path filePath, int virtualFiles, boolean readOnly, PageCache pageCache) {
        this(filePath, virtualFiles, pageCache.getPageSize(), readOnly, pageCache);
    }

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

        headerSize = virtualFiles * HEADER_RECORD_SIZE;
        tableSize = virtualFiles * PAGES_PER_VIRUAL_FILE * 8;

        try {
            headerBuffer = channel.map(mapMode, 0, headerSize);
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

        long nextPosition = Arrays.stream(lastPagePositions).mapToLong(AtomicLong::get).max().orElse(0L);

        try {
            pageTableBuffer = channel.map(mapMode, headerSize, tableSize);
            pageTable = pageTableBuffer.asLongBuffer();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map page locations for path: " + filePath, e);
        }

        if (nextPosition == 0) {
            nextPagePosition = new AtomicLong(headerSize + tableSize);

        } else if (nextPosition < headerSize ){
            throw new IllegalStateException("file position " + nextPosition + " is less than header size: " + headerSize + " in file " + filePath);
        } else {
            nextPagePosition = new AtomicLong(nextPosition);
        }

        // TODO Check file size on startup and try to recover pages?
    }

    public int getVirtualFiles() {
        return virtualFiles;
    }
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Get the position in the page for the virtual file position
     * @param pos the position in the virtual file
     * @return the position in the page
     */
    public int pagePosition(long pos) {
        return (int) (pos % (long) pageSize);
    }

    /**
     * Get the page number in the virtual file for a given position
     * @param pos the position in the virtual file
     * @return the page this position occurs in
     */
    public int pageNumber(long pos) {
        return (int) (pos / (long) pageSize);
    }

    /**
     * Get a FileChannel backed Page (no cache)
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    public Page getFilePage(int virtualFileNumber, int pageNumber) {
        long startPosition = getOrAllocatePage(virtualFileNumber, pageNumber);

        return filePage(startPosition);
    }

    /**
     * Get a MappedByteBuffer backed Page if there is a cache and the page exists. Otherwise it returns a FilePage
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    public Page getPage(int virtualFileNumber, int pageNumber) {
        long startPosition = getOrAllocatePage(virtualFileNumber, pageNumber);

        if (pageCache != null) {
            return pageCache.getIfPresent(this, startPosition).orElse(filePage(startPosition));
        } else {
            return filePage(startPosition);
        }
    }

    /**
     * Get a MappedByteBuffer backed Page uses a page cache if present
     * @param virtualFileNumber the virtual file number
     * @param pageNumber the page number to getValue
     * @return a Page for File IO
     */
    public Page getMappedPage(int virtualFileNumber, int pageNumber) {
        long startPosition = getOrAllocatePage(virtualFileNumber, pageNumber);


        if (pageCache != null) {
            return pageCache.get(this, startPosition);
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
        return new FilePage(channel,startPosition + 8, pageSize);
    }


    long getPageStart(int virtualFileNumber, int pageNumber) {
        if (pageNumber == -1) return -1L;
        int index = PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber;
        return pageTable.get(index);
    }

    void putPageStart(int virtualFileNumber, int pageNumber, long position){
        int index = PAGES_PER_VIRUAL_FILE * virtualFileNumber + pageNumber;
        pageTable.put(index, position);
    }

    long getHeaderFirstPage(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE);
    }

    void putHeaderFirstPage(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE, position);
    }

    long getHeaderLastPage(int virtualFileNumber) {
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE + 8);
    }

    void putHeaderLastPage(int virtualFileNumber, long position) {
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE + 8, position);
    }

    long getHeaderVirtualFilePosition(int virtualFileNumber){
        return headerBuffer.getLong(virtualFileNumber * HEADER_RECORD_SIZE + 16);
    }

    void putHeaderVirtualFilePosition(int virtualFileNumber, long position){
        headerBuffer.putLong(virtualFileNumber * HEADER_RECORD_SIZE + 16, position);
    }

    AtomicLong getAtomicVirtualFilePosition(int virtualFileNumber){
        return virtualFilePositions[virtualFileNumber];
    }

    
    int getHeaderVirtualFilePageCount(int virtualFileNumber) {
        return headerBuffer.getInt(virtualFileNumber * HEADER_RECORD_SIZE + 24);
    }

    void putHeaderVirtualFilePageCount(int virtualFileNumber, int count) {
        headerBuffer.putInt(virtualFileNumber * HEADER_RECORD_SIZE + 24, count);
    }

    long getOrAllocatePage(int virtualFileNumber, int pageNumber) {
        final long startPosition;
        int curentPageCount = virtualFilePageCounts[virtualFileNumber].get();
        if (pageNumber < curentPageCount) {
            return getPageStart(virtualFileNumber, pageNumber);
        } else if (pageNumber == curentPageCount) {
            synchronized (virtualFilePageCounts[virtualFileNumber]) {
                if (pageNumber == virtualFilePageCounts[virtualFileNumber].get()) {
                    return allocatePage(virtualFileNumber);
                }
            }
            return getPageStart(virtualFileNumber, pageNumber);
        } else {
            throw new IllegalStateException("Requested page " + pageNumber + " but only currently have " + curentPageCount);
        }
    }


    long allocatePage(int virtualFileNumber) {

        // Do the atomic stuff
        long nextPageStart = nextPagePosition.getAndAdd(pageSize + 16);
        int nextPageNumber = virtualFilePageCounts[virtualFileNumber].getAndIncrement(); // the result is the index, the incremented value is the new count!
        boolean firstPage = firstPagePositions[virtualFileNumber].compareAndSet(0L, nextPageStart);
        lastPagePositions[virtualFileNumber].set(nextPageStart);

        // Update the persisted header values
        if (firstPage) putHeaderFirstPage(virtualFileNumber, nextPageStart);
        putHeaderLastPage(virtualFileNumber, nextPageStart);
        putHeaderVirtualFilePageCount(virtualFileNumber, nextPageNumber + 1);

        // Update the persistent table of pages
        putPageStart(virtualFileNumber, nextPageNumber, nextPageStart);

        long lastPageStart = getPageStart(virtualFileNumber, nextPageNumber -1);

        ByteBuffer longBuffer = LOCAL_LONG_BUFFER.get();

        // put the forward pointer in the old page
        if (lastPageStart > 0) {
            longBuffer.asLongBuffer().put(nextPageStart);
            longBuffer.rewind();
            try {
                channel.write(longBuffer, lastPageStart + pageSize + 8);
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to next pointer for new page in " + filePath, e);
            }
        }

        // Put the tail pointer in the new page
        longBuffer.rewind();
        longBuffer.asLongBuffer().put(lastPageStart); // will be -1 if this is the first page
        longBuffer.rewind();
        try {
            channel.write(longBuffer, nextPageStart);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to next pointer for new page in " + filePath, e);
        }

        return nextPageStart;
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

    public void clear() throws IOException {

        if (pageCache != null) pageCache.flush();

        IntStream
                .range(0, virtualFiles)
                .forEach(virtualFileNumber -> {
                    putHeaderFirstPage(virtualFileNumber, 0);
                    firstPagePositions[virtualFileNumber].set(0);

                    putHeaderLastPage(virtualFileNumber, 0);
                    lastPagePositions[virtualFileNumber].set(0);

                    putHeaderVirtualFilePosition(virtualFileNumber, 0);
                    virtualFilePositions[virtualFileNumber].set(0);

                    putHeaderVirtualFilePageCount(virtualFileNumber, 0);
                    virtualFilePageCounts[virtualFileNumber].set(0);
                });

        nextPagePosition.set(headerSize + tableSize);

        channel.truncate(headerSize + tableSize);
    }

}
