package com.upserve.uppend;

import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.atomic.*;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path file;

    private final FileChannel blobs;
    private final AtomicLong blobPosition;

    protected static final int PAGE_SIZE = 256 * 1024; // allocate 256 KB chunks
    private static final int MAX_PAGES = 1024 * 1024; // max 4 TB (~800 MB heap)
    private final MappedByteBuffer[] pages;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean readOnly;

    public Blobs(Path file) {
        this(file, false);
    }

    public Blobs(Path file, boolean readOnly) {
        this.file = file;

        this.readOnly = readOnly;

        pages = new MappedByteBuffer[MAX_PAGES];

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ};
        } else {
            openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }

        try {
            blobs = FileChannel.open(file, openOptions);
            blobPosition = new AtomicLong(blobs.size());
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blob file: " + file, e);
        }
    }

    public long append(byte[] buf) {
        int writeSize = buf.length + 4;

        final long pos = blobPosition.getAndAdd(writeSize);

        ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        intBuf.putInt(buf.length).flip();

        int wrote;
        wrote = write(pos, intBuf.array());

        wrote = write(pos+4, buf);

        log.trace("appended {} readBuffer to {} at pos {}", buf.length, file, pos);
        return pos;
    }

    private int write(long pos, byte[] bytes){
        return write(pos, 0, bytes);
    }

    private int write(long pos, int offset, byte[] buf) {

        int pagePos = (int) (pos % (long) PAGE_SIZE);

        MappedByteBuffer currentPage = page(pos);
        int pageRemaining = PAGE_SIZE - pagePos;
        int bytesToWrite = buf.length - offset;

        int writeLength = bytesToWrite < pageRemaining ? bytesToWrite : pageRemaining;

        synchronized (currentPage) {
            currentPage.position(pagePos);
            currentPage.put(buf, offset, writeLength);
        }

        if (writeLength < bytesToWrite){
            writeLength += write(pos + writeLength, offset + writeLength, buf);
        }
        return writeLength;
    }

    private MappedByteBuffer page(long pos) {
        long pageIndexLong = pos / PAGE_SIZE;
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
                long pageStart = (long) pageIndex * PAGE_SIZE;
                try {
                    page = blobs.map(readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, pageStart, PAGE_SIZE);
                } catch (IOException e) {
                    throw new UncheckedIOException("unable to map page at page index " + pageIndex + " (" + pageStart + " + " + PAGE_SIZE + ") in " + file, e);
                }
                pages[pageIndex] = page;
            }
        }
        return page;
    }

    public long size(){
        if (readOnly) {
            try {
                return blobs.size();
            } catch (IOException e) {
                log.error("Unable to get blobs file size: " + file, e);
                return -1;
            }
        } else {
            return blobPosition.get();
        }
    }

    public byte[] read(long pos) {
        log.trace("reading from {} @ {}", file, pos);

        int size = readInt(pos);
        byte[] buf = new byte[size];
        read(pos + 4, buf);
        log.trace("read {} readBuffer from {} @ {}", size, file, pos);
        return buf;
    }

    public void clear() {
        log.trace("clearing {}", file);
        try {
            Arrays.fill(pages, null);
            blobs.truncate(0);
            blobPosition.set(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() {
        log.trace("closing {}", file);
        closed.set(true);
        try {
            flush();
            blobs.close();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to close blobs " + file, e);
        }
    }

    @Override
    public void flush() {
        if (readOnly) return;
        for (MappedByteBuffer page : pages) {
            if (page != null) {
                page.force();
            }
        }
    }

    private int readInt(long pos) {
        byte[] buf = new byte[4];
        read(pos, buf);
        return Ints.fromByteArray(buf);
    }

    private int read(long pos, byte[] buf){
        return read(pos, 0, buf);
    }

    private int read(long pos, int offset, byte[] buf) {
        int pagePos = (int) (pos % (long) PAGE_SIZE);
        MappedByteBuffer currentPage = page(pos);

        int pageRemaining = PAGE_SIZE - pagePos;
        int bytesToRead = buf.length - offset;

        int readLength = bytesToRead < pageRemaining ? bytesToRead : pageRemaining;

        synchronized (currentPage) {
            currentPage.position(pagePos);
            currentPage.get(buf, offset, readLength);
        }

        if (readLength < bytesToRead){
            readLength += read(pos + readLength, offset + readLength, buf);
        }
        return readLength;
    }
}
