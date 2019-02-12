package com.upserve.uppend;

import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.stream.*;

public class BlockedLongs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Use a large prime
    private static final int LOCK_SIZE = 10007;
    private final Striped<Lock> stripedLocks;

    static final int HEADER_SIZE = 128; // Reserve 128 bytes of header.
    // Currently uses 8 for position, 4 for values per block
    private static final int IDEAL_PAGE_SIZE = 128 * 1024 * 1024; // allocate 128 MB chunks
    private static final int MAX_PAGES = 32 * 1024; // max 4 TB

    private final Path file;

    private final int valuesPerBlock;
    private final int blockSize;
    private final int page_size;

    private final FileChannel blocks;
    private final MappedByteBuffer[] pages;

    private final MappedByteBuffer headerBuffer;
    private final AtomicLong posMem;

    private final AtomicInteger currentPage;
    private final boolean readOnly;

    private final LongAdder appendCounter;
    private final LongAdder allocCounter;
    private final LongAdder valuesReadCounter;


    public BlockedLongs(Path file, int valuesPerBlock, boolean readOnly) {
        if (file == null) {
            throw new IllegalArgumentException("null file");
        }

        this.file = file;
        this.readOnly = readOnly;

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        if (valuesPerBlock < 1) {
            throw new IllegalArgumentException("bad (< 1) values per block: " + valuesPerBlock);
        }

        appendCounter = new LongAdder();
        allocCounter = new LongAdder();
        valuesReadCounter = new LongAdder();

        this.valuesPerBlock = valuesPerBlock;

        blockSize = 16 + valuesPerBlock * 8;
        page_size = (IDEAL_PAGE_SIZE / blockSize) * blockSize;

        StandardOpenOption[] openOptions;
        if (readOnly){
            openOptions = new StandardOpenOption[]{StandardOpenOption.READ};
        } else {
            openOptions = new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }

        try {
            blocks = FileChannel.open(file, openOptions);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blocks file: " + file, e);
        }

        if (readOnly) {
            stripedLocks = null;
        } else {
            stripedLocks = Striped.lock(LOCK_SIZE);
        }

        pages = new MappedByteBuffer[MAX_PAGES];

        currentPage = new AtomicInteger(0);
        ensurePage(0);

        try {
            headerBuffer = blocks.map(readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to map pos buffer in " + file, e);
        }

        long pos = headerBuffer.getLong(0);
        int header_vals_per_block = headerBuffer.getInt(8);
        if (pos < 0) {
            throw new IllegalStateException("negative pos (" + pos + "): " + file);
        }

        try {
            if (pos > blocks.size()) {
                throw new IllegalStateException("pos (" + pos + ") > size of " + file + " is > than (" + blocks.size() + ")");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get block file size: " + file, e);
        }

        if (pos == 0 && !readOnly) {
            pos = HEADER_SIZE;
            headerBuffer.putLong(0, pos);
            headerBuffer.putInt(8, valuesPerBlock);
        } else if (valuesPerBlock != header_vals_per_block){
            throw new IllegalStateException(
                    "Values per block header " + header_vals_per_block + " does not match configured value " +
                            valuesPerBlock + " in file " + file
            );
        }
        posMem = new AtomicLong(pos);
    }

    /**
     * Allocate a new block of longs
     *
     * @return the position of the new block
     */
    public long allocate() {
        log.trace("allocating block of {} bytes in {}", blockSize, file);
        allocCounter.increment();
        long pos = posMem.getAndAdd(blockSize);
        headerBuffer.putLong(0, posMem.get());
        return pos;
    }

    /**
     * get some stats about the blocked long store
     * @return Stats about activity in this BlockedLongs
     */
    public BlockStats stats() {
        return new BlockStats(currentPage.get() , size(), appendCounter.longValue(), allocCounter.longValue(), valuesReadCounter.longValue());
    }

    /**
     * Return the current number of bytes which have been allocated for blocks
     *
     * @return the number of bytes
     */
    public long size() {
        if (readOnly) {
            return headerBuffer.getLong(0);
        } else {
            return posMem.get();
        }
    }

    public void append(final long pos, final long val) {
        log.trace("appending value {} to {} at {}", val, file, pos);
        if (readOnly) throw new RuntimeException("Can not append a read only blocked longs file: " + file);
        // size | -next
        // prev | -last

        appendCounter.increment();

        Lock lock = stripedLocks.getAt((int) (pos % LOCK_SIZE));
        lock.lock();
        try {
            final long prev = readLong(pos + 8);
            if (prev > 0) {
                throw new IllegalStateException("append called at non-starting block: pos=" + pos + " in path: " + file);
            }
            long last = prev == 0 ? pos : -prev;
            long size = readLong(last);
            if (size < 0) {
                log.debug("Read repair for last block with a next: pos=" + pos + " in path: " + file);
                // The the new position was set and this block is full, but is not updated yet
                last = -size;
                size = readLong(last);
                writeLong(pos + 8, -last);
            }

            if (size > valuesPerBlock) {
                throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + size + ": pos=" + pos + " in path: " + file);
            }
            if (size == valuesPerBlock) {
                long newPos = allocate();
                // write new value in new block
                writeLong(newPos, 1);
                writeLong(newPos + 8, last);
                writeLong(newPos + 16, val);
                // link to last->next
                writeLong(last, -newPos);
                // link to first->last
                writeLong(pos + 8, -newPos);
            } else {
                writeLong(last + 16 + 8 * size, val);
                writeLong(last, size + 1);
            }
        } finally {
            lock.unlock();
        }
        log.trace("appended value {} to {} at {}", val, file, pos);
    }

    public LongStream values(Long pos) {
        log.trace("streaming values from {} at {}", file, pos);

        valuesReadCounter.increment();

        if (pos == null) {
            // pos will be null for missing keys
            return LongStream.empty();
        }

        if (pos < HEADER_SIZE) {
            throw new IllegalArgumentException("The position " + pos + " is less than the header size");
        }

        long[] firstBlock = readLongs(pos);

        // size | -next [pos]
        // prev | -last [pos + 8]
        final long sizeOrNext = firstBlock[0];

        if (sizeOrNext > 0) {
            if (sizeOrNext > valuesPerBlock) {
                throw new IllegalStateException("Invalid block size " + sizeOrNext + " at pos " + pos + " in file " + file);
            }
            return Arrays.stream(firstBlock, 2, (int) sizeOrNext + 2);

        } else if (sizeOrNext < 0) {
            // it is next, size of this block is valuesPerBlock
            long nextPos = -sizeOrNext;
            final long lastPos = -firstBlock[1];

            long[] lastBlock = readLongs(lastPos);
            int lastBlockSize = lastBlock[0] > 0 ? (int) lastBlock[0] : valuesPerBlock;

            if (nextPos == lastPos) {
                // Just read the last block and concat
                return LongStream.concat(
                    Arrays.stream(firstBlock, 2, valuesPerBlock + 2),
                    Arrays.stream(lastBlock, 2, lastBlockSize + 2)
                );

            } else {
                // Make two iterators one forward one backward.
                // Iterator should stop when the values cross - block positions are sorted by definition

                AtomicLong forwardPosition = new AtomicLong(nextPos);
                AtomicLong backwardPosition = new AtomicLong(lastPos)

                ForwardBlockReader forwardBlockReader = new ForwardBlockReader(this, nextPos, At)



            }

        } else {
            throw new IllegalStateException("Size should never be zero");
        }
    }


    class ForwardBlockReader implements Runnable {
        private final BlockedLongs blockedLongs;
        AtomicLong forwardPosition;
        long lastRead;
        long nextRead;
        private final AtomicLong backwardPosition;
        final List<long[]> blocks;

        public ForwardBlockReader(BlockedLongs blockedLongs, AtomicLong forwardPosition, AtomicLong backwardPosition) {
            this.blockedLongs = blockedLongs;
            this.forwardPosition = forwardPosition;
            this.backwardPosition = backwardPosition;
            this.blocks = new ArrayList<>();
            this.nextRead = forwardPosition.get();
        }

        @Override
        public void run() {
            while(nextRead < backwardPosition.get()) {
                long[] block = blockedLongs.readLongs(nextRead);
                blocks.add(block);
                lastRead = nextRead;
                nextRead = -block[0];
                forwardPosition.set(nextRead);
            }
        }
    }

    class BackwardBlockReader implements Runnable {
        private final BlockedLongs blockedLongs;
        private final AtomicLong forwardPosition;
        AtomicLong backwardPosition;
        long lastRead;
        long nextRead;
        final List<long[]> blocks;

        public BackwardBlockReader(BlockedLongs blockedLongs, AtomicLong forwardPosition, AtomicLong backwardPosition) {
            this.blockedLongs = blockedLongs;
            this.forwardPosition = forwardPosition;
            this.backwardPosition = backwardPosition;
            this.blocks = new ArrayList<>();
            nextRead = backwardPosition.get();
        }

        @Override
        public void run() {
            while(forwardPosition.get() < nextRead) {
                long[] block = blockedLongs.readLongs(nextRead);
                blocks.add(block);
                lastRead = nextRead;
                nextRead = -block[1];
                backwardPosition.set(nextRead);
            }
        }
    }

    public long[] valuesArray(Long pos) {

        if (pos < 0 || pos >= size()) {
            throw new IllegalArgumentException("valuesArray requested with position " + pos +" greater than the file size " + size() + " in file " + file);
        }

        // size | -next
        // prev | -last
        final long size = readLong(pos);

        if (size < 0) {
            long nextPos = -size;
            long[] values = new long[valuesPerBlock];
            for (int i = 0; i < valuesPerBlock; i++) {
                values[i] = readLong(pos + 16 + i * 8);
            }

            long[] additionalValues = valuesArray(nextPos);

            if (additionalValues.length == 0){
                return values;
            } else {
                long[] result = new long[valuesPerBlock + additionalValues.length];
                System.arraycopy(values,0,result,0, valuesPerBlock);
                System.arraycopy(additionalValues,0,result,valuesPerBlock, additionalValues.length);
                return result;
            }
        } else if (size > valuesPerBlock) {
            throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + size);
        } else if (size == 0) {
            return new long[]{};
        } else {
            int numValues = (int) size;
            long[] values = new long[numValues];
            for (int i = 0; i < numValues; i++) {
                values[i] = readLong(pos + 16 + i * 8);
            }
            return values;
        }
    }

    public long lastValue(long pos) {
        log.trace("reading last value from {} at {}", file, pos);

        if (pos < 0 || pos >= size()) {
            throw new IllegalArgumentException("valuesArray requested with position " + pos +" greater than the file size " + size() + " in file " + file);
        }

        // size | -next
        // prev | -last

        final long prev = readLong(pos + 8);
        if (prev > 0) {
            throw new IllegalStateException("lastValue called at non-starting block: pos=" + pos);
        }
        long last = prev == 0 ? pos : -prev;
        long size = readLong(last);
        if (size == 0) {
            if (prev < 0) {
                throw new IllegalStateException("got to empty last block: pos=" + pos);
            }
            return -1;
        }
        if (size < 0) {
            log.debug("Read recovery for last block (at " + last + "): pos=" + pos);
            last = -size;
            size = readLong(last);
        }
        if (size > valuesPerBlock) {
            throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + size + ": pos=" + pos);
        }
        long value = readLong(last + 16 + 8 * (size - 1));
        log.trace("got value from {} at {}: {}", file, pos, value);
        return value;
    }

    public void clear() {
        log.debug("clearing {}", file);
        IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).lock());
        try {
            blocks.truncate(HEADER_SIZE);
            headerBuffer.putLong(0, HEADER_SIZE);
            posMem.set(HEADER_SIZE);
            Arrays.fill(pages, null);
            ensurePage(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        } finally {
            IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).unlock());
        }
    }

    @Override
    public void close() throws IOException {
        log.debug("closing {}", file);

        if (readOnly) {
            blocks.close();
            return;
        }

        IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).lock());
        try {
            flush();
            blocks.close();
        } finally {
            IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).unlock());
        }
    }

    @Override
    public void flush() {
        if (readOnly) return;
        log.debug("flushing {}", file);
        headerBuffer.force();

        Arrays.stream(pages)
                .parallel()
                .filter(Objects::nonNull)
                .forEach(MappedByteBuffer::force);

        log.debug("flushed {}", file);
    }

    public void trim() {
        IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).lock());
        try {
            flush();
            Arrays.fill(pages, null);
            currentPage.set(0);
            ensurePage(0);
        } finally {
            IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).unlock());
        }
    }

    private long[] readLongs(long pos) {
        return readLongs(pos, valuesPerBlock + 2);
    }

    private long[] readLongs(long pos, int size) {
        LongBuffer buffer = page(pos).duplicate().asLongBuffer();
        buffer.position(pagePos(pos) / 8);

        long[] dst = new long[size];
        buffer.get(dst);
        return dst;
    }

    private long readLong(long pos) {
        int ppos = pagePos(pos);
        return page(pos).getLong(ppos);
    }

    void writeLong(long pos, long val) {
        page(pos).putLong(pagePos(pos), val);
    }

    private int pagePos(long pos) {
        return (int) ((pos - HEADER_SIZE) % (long) page_size);
    }

    private MappedByteBuffer page(long pos) {
        long pageIndexLong = (pos - HEADER_SIZE) / page_size;
        if (pageIndexLong > Integer.MAX_VALUE) {
            throw new RuntimeException("page index exceeded max int: " + pageIndexLong);
        }
        int pageIndex = (int) pageIndexLong;

        return ensurePage(pageIndex);
    }

    private MappedByteBuffer ensurePage(int pageIndex) {
        MappedByteBuffer page = pages[pageIndex];
        if (page == null) {
            synchronized (pages) {
                page = pages[pageIndex];
                if (page == null) {

                    currentPage.getAndUpdate(last -> pageIndex > last ? pageIndex : last);

                    long pageStart = (long) pageIndex * page_size + HEADER_SIZE;
                    try {
                        FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
                        page = blocks.map(mapMode, pageStart, page_size);

                    } catch (IOException e) {
                        throw new UncheckedIOException("unable to map page at page index " + pageIndex + " (" + pageStart + " + " + page_size + ") in " + file, e);
                    }
                    pages[pageIndex] = page;
                }
            }
        }
        return page;
    }
}
