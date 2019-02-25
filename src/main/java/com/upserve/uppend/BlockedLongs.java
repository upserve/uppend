package com.upserve.uppend;

import com.google.common.util.concurrent.Striped;
import com.upserve.uppend.util.*;
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

    private static final int PAGE_SIZE = 128 * 1024 * 1024; // allocate 128 MB chunks
    private static final int MAX_PAGES = 32 * 1024; // max 4 TB

    private final Path file;

    private final int valuesPerBlock;
    private final int blockSize;

    private final FileChannel blocks;
    private final MappedByteBuffer[] pages;

    private final FileChannel blocksPos;
    private final MappedByteBuffer posBuf;
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

        Path posFile = dir.resolve(file.getFileName() + ".pos");

        if (valuesPerBlock < 1) {
            throw new IllegalArgumentException("bad (< 1) values per block: " + valuesPerBlock);
        }

        appendCounter = new LongAdder();
        allocCounter = new LongAdder();
        valuesReadCounter = new LongAdder();

        this.valuesPerBlock = valuesPerBlock;
        blockSize = 16 + valuesPerBlock * 8;

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

        ensurePage(0);
        currentPage = new AtomicInteger(0);

        try {
            blocksPos = FileChannel.open(posFile, openOptions);
            if (blocksPos.size() > 8) {
                throw new IllegalStateException("bad (!= 8) size for block pos file: " + posFile);
            }
            try {
                posBuf = blocksPos.map(readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, 8);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to map pos buffer at in " + posFile, e);
            }
            long pos = posBuf.getLong(0);
            if (pos < 0) {
                throw new IllegalStateException("negative pos (" + pos + "): " + posFile);
            }
            if (pos > blocks.size()) {
                throw new IllegalStateException("pos (" + pos + ") > size of " + file + " (" + blocks.size() + "): " + posFile);
            }
            posMem = new AtomicLong(pos);


        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blocks pos file: " + posFile, e);
        }
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
        posBuf.putLong(0, posMem.get());
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
            return posBuf.getLong(0);
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

    public LongStream values(Long pos){
        log.trace("streaming values from {} at {}", file, pos);

        valuesReadCounter.increment();

        if (pos == null) {
            // pos will be null for missing keys
            return LongStream.empty();
        }

        long[] longs = valuesArray(pos);
        return Arrays.stream(longs);
    }

    public long[] valuesArray(Long pos) {

        if (pos < 0 || pos > size()) {
            log.error("Bad position value {} in file {} of size {}", pos, file, size());
            return new long[]{};
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

    // Lazy values is much slower in Performance tests with a large number of blocks.
    // Might be workable with a custom spliterator that was block aware for parallelization
    public LongStream lazyValues(Long pos) {
        log.trace("streaming values from {} at {}", file, pos);

        valuesReadCounter.increment();

        if (pos == null) {
            // pos will be null for missing keys
            return LongStream.empty();
        }

        if (pos < 0 || pos > size()) {
            log.error("Bad position value {} in file {} of size {}", pos, file, size());
            return LongStream.empty();
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
            return LongStreams.lazyConcat(Arrays.stream(values), () -> values(nextPos));
        } else if (size > valuesPerBlock) {
            throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + size);
        } else if (size == 0) {
            return LongStream.empty();
        } else {

            int numValues = (int) size;
            long[] values = new long[numValues];
            for (int i = 0; i < numValues; i++) {
                values[i] = readLong(pos + 16 + i * 8);
            }
            if (log.isTraceEnabled()) {
                String valuesStr = Arrays.toString(values);
                log.trace("got values from {} at {}: {}", file, pos, valuesStr);
            }
            return Arrays.stream(values);
        }
    }

    public long lastValue(long pos) {
        log.trace("reading last value from {} at {}", file, pos);

        if (pos >= posMem.get()) {
            return -1;
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
            blocks.truncate(0);
            posBuf.putLong(0, 0);
            posMem.set(0);
            Arrays.fill(pages, null);
            currentPage.set(0);
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
            blocksPos.close();
            return;
        }

        IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).lock());
        try {
            flush();
            blocks.close();
            blocksPos.close();
        } finally {
            IntStream.range(0, LOCK_SIZE).forEach(index -> stripedLocks.getAt(index).unlock());
        }
    }

    @Override
    public void flush() {
        if (readOnly) return;
        log.debug("flushing {}", file);
        posBuf.force();

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

    private long readLong(long pos) {
        int pagePos = (int) (pos % (long) PAGE_SIZE);
        return page(pos).getLong(pagePos);
    }

    protected void writeLong(long pos, long val) {
        int pagePos = (int) (pos % (long) PAGE_SIZE);
        page(pos).putLong(pagePos, val);
    }

    private MappedByteBuffer page(long pos) {
        long pageIndexLong = pos / PAGE_SIZE;
        if (pageIndexLong > Integer.MAX_VALUE) {
            throw new RuntimeException("page index exceeded max int: " + pageIndexLong);
        }
        int pageIndex = (int) pageIndexLong;

        MappedByteBuffer page = ensurePage(pageIndex);
        preloadPage(pageIndex + 1);
        return page;
    }

    private void preloadPage(int pageIndex) {
        if (pageIndex < MAX_PAGES && pages[pageIndex] == null) {
            // preload page
            int prev = currentPage.getAndUpdate(current -> current < pageIndex ? pageIndex : current);
            if (prev < pageIndex) {
                ensurePage(pageIndex);
            }
        }
    }

    private MappedByteBuffer ensurePage(int pageIndex) {
        MappedByteBuffer page = pages[pageIndex];
        if (page == null) {
            synchronized (pages) {
                page = pages[pageIndex];
                if (page == null) {
                    long pageStart = (long) pageIndex * PAGE_SIZE;
                    try {
                        FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
                        page = blocks.map(mapMode, pageStart, PAGE_SIZE);
                    } catch (IOException e) {
                        throw new UncheckedIOException("unable to map page at page index " + pageIndex + " (" + pageStart + " + " + PAGE_SIZE + ") in " + file, e);
                    }
                    pages[pageIndex] = page;
                }
            }
        }
        return page;
    }
}
