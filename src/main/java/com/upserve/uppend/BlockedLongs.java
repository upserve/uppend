package com.upserve.uppend;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.LongStream;

@Slf4j
public class BlockedLongs implements AutoCloseable {
    private static final int PAGE_SIZE = 4 * 1024 * 1024; // allocate 4 MB chunks
    private static final int MAX_PAGES = 1024 * 1024; // max 4 TB (~800 MB heap)

    private final Path file;

    private final int valuesPerBlock;
    private final int blockSize;

    private final FileChannel blocks;
    private final MappedByteBuffer[] pages;

    private final Supplier<ByteBuffer> bufferLocal;

    private final FileChannel blocksPos;
    private final MappedByteBuffer posBuf;

    public BlockedLongs(Path file, int valuesPerBlock) {
        this.file = file;

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        Path posFile = dir.resolve(file.getFileName() + ".pos");

        this.valuesPerBlock = valuesPerBlock;
        blockSize = (valuesPerBlock + 1) * 8;

        try {
            blocks = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blocks file: " + file, e);
        }

        pages = new MappedByteBuffer[MAX_PAGES];

        bufferLocal = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(blockSize);

        try {
            blocksPos = FileChannel.open(posFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (blocksPos.size() > 8) {
                throw new IllegalStateException("bad (!= 8) size for block pos file: " + posFile);
            }
            try {
                posBuf = blocksPos.map(FileChannel.MapMode.READ_WRITE, 0, 8);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to map pos buffer at in " + posFile, e);
            }
            long pos = getPos();
            if (pos < 0) {
                throw new IllegalStateException("negative pos (" + pos + "): " + posFile);
            }
            if (pos > blocks.size()) {
                throw new IllegalStateException("pos (" + pos + ") > size of " + file + " (" + blocks.size() + "): " + posFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blocks pos file: " + posFile, e);
        }
    }

    public synchronized long allocate() {
        log.trace("allocating {} bytes in {}", blockSize, file);
        synchronized (posBuf) {
            long pos = getPos();
            putPos(pos + blockSize);
            return pos;
        }
    }

    // TODO: find way to remove synchronized on this method
    public synchronized void append(long pos, long val) {
        log.trace("appending value {} to {} at {}", val, file, pos);
        long numValuesLong = readLong(pos);
        if (numValuesLong < 0) {
            long nextPos = -numValuesLong;
            append(nextPos, val);
            // TODO: avoid navigating a long list by having a tail pointer?
        } else {
            if (numValuesLong > valuesPerBlock) {
                throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + numValuesLong);
            }
            if (numValuesLong == valuesPerBlock) {
                long newBlockPos = allocate();
                append(newBlockPos, val);
                writeLong(pos, -newBlockPos);
            } else {
                long valuePos = pos + 8 + (numValuesLong * 8);
                writeLong(valuePos, val);
                writeLong(pos, numValuesLong + 1);
            }
        }
        log.trace("appended value {} to {} at {}", val, file, pos);
    }

    public LongStream values(long pos) {
        log.trace("streaming values from {} at {}", file, pos);
        if (pos >= getPos()) {
            return LongStream.empty();
        }
        ByteBuffer buf = readBlock(pos);
        if (buf == null) {
            return LongStream.empty();
        }
        buf.flip();
        long numValuesLong = buf.getLong();
        if (numValuesLong < 0) {
            long nextPos = -numValuesLong;
            long[] values = new long[valuesPerBlock];
            for (int i = 0; i < valuesPerBlock; i++) {
                values[i] = buf.getLong();
            }
            return LongStream.concat(Arrays.stream(values), values(nextPos));
        } else {
            if (numValuesLong > valuesPerBlock) {
                throw new IllegalStateException("too high num values: expected <= " + valuesPerBlock + ", got " + numValuesLong);
            }
            int numValues = (int) numValuesLong;
            long[] values = new long[numValues];
            for (int i = 0; i < numValues; i++) {
                values[i] = buf.getLong();
            }
            if (log.isTraceEnabled()) {
                String valuesStr = Arrays.toString(values);
                log.trace("got values from {} at {}: {}", file, pos, valuesStr);
            }
            return Arrays.stream(values);
        }
    }

    public void clear() {
        log.debug("clearing {}", file);
        try {
            blocks.truncate(0);
            putPos(0);
            Arrays.fill(pages, null);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() throws Exception {
        log.trace("closing {}", file);
        for (MappedByteBuffer page : pages) {
            if (page != null) {
                page.force();
            }
        }
        blocks.close();
        posBuf.force();
        blocksPos.close();
    }

    private ByteBuffer readBlock(long pos) {
        ByteBuffer buf = bufferLocal.get();
        try {
            int numRead = blocks.read(buf, pos);
            if (numRead == -1) {
                return null;
            }
            if (numRead != blockSize) {
                throw new RuntimeException("read bad block size from " + file + " at pos " + pos + ": got " + numRead + ", expected " + blockSize);
            }
        } catch (Exception e) {
            throw new RuntimeException("unable to read block at pos " + pos + ": " + file, e);
        }
        return buf;
    }

    private long readLong(long pos) {
        int pagePos = (int) (pos % (long) PAGE_SIZE);
        return page(pos).getLong(pagePos);
    }

    private void writeLong(long pos, long val) {
        int pagePos = (int) (pos % (long) PAGE_SIZE);
        page(pos).putLong(pagePos, val);
    }

    private MappedByteBuffer page(long pos) {
        long pageIndexLong = pos / PAGE_SIZE;
        if (pageIndexLong > Integer.MAX_VALUE) {
            throw new RuntimeException("page index exceeded max int: " + pageIndexLong);
        }
        int pageIndex = (int) pageIndexLong;
        MappedByteBuffer page = pages[pageIndex];
        if (page == null) {
            long pageStart = (long) pageIndex * PAGE_SIZE;
            try {
                page = blocks.map(FileChannel.MapMode.READ_WRITE, pageStart, PAGE_SIZE);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to map page at pos " + pos + " (" + pageStart + " + " + PAGE_SIZE + ") in " + file, e);
            }
            pages[pageIndex] = page;
        }
        return page;
    }

    private long getPos() {
        return posBuf.getLong(0);
    }

    private void putPos(long pos) {
        posBuf.putLong(0, pos);
    }
}
