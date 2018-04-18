package com.upserve.uppend.blobs;

import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class PageMappedFileIO implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Supplier<ByteBuffer> LOCAL_INT_BUFFER = ThreadLocalByteBuffers.LOCAL_INT_BUFFER;
    static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER;

    final Path filePath;
    final PageCache pageCache;

    private final MappedByteBuffer posBuf;


    final FileCache fileCache;
    final AtomicLong position;

    PageMappedFileIO(Path file, PageCache pageCache) {
        this.filePath = file;
        this.pageCache = pageCache;

        this.fileCache = pageCache.getFileCache();

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        try {
            posBuf = fileCache.getFileChannel(file).map(fileCache.readOnly() ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, 8);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to open paged file for path: " + file, e);
        }

        long pos = posBuf.getLong(0);
        if (pos < 0) {
            throw new IllegalStateException("file position is less than zero: " + pos);
        } else if (pos == 0) {
            pos = 8;
        }

        log.debug("Opening page mapped file with position {}", pos);
        position = new AtomicLong(pos);
    }

    long appendPosition(int size) {
        // return the position to write at
        final long result = position.getAndAdd(size);
        // record the position written too
        posBuf.putLong(0, result+size);
        return result;
    }

    public long getPosition(){
        if (fileCache.readOnly()){
            return posBuf.getLong(0);
        } else{
            return position.get();
        }
    }

    public void clear() {
        log.trace("clearing {}", filePath);
        try {
            position.set(8);
            posBuf.putLong(0,8);
            fileCache.getFileChannel(filePath).truncate(8);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    void writeMappedInt(long pos, int val){
        ByteBuffer intBuf = LOCAL_INT_BUFFER.get();
        intBuf.putInt(val).flip();
        writeMapped(pos, intBuf.array());
    }

    void writeMappedLong(long pos, long val){
        ByteBuffer longBuf = LOCAL_LONG_BUFFER.get();
        longBuf.putLong(val).flip();
        writeMapped(pos, longBuf.array());
    }

    void writeMapped(long pos, byte[] bytes){
        if (bytes.length == 0) {
            return;
        }
        final int result = writeMappedOffset(pos, bytes, 0);
        if (result != bytes.length) {
            throw new RuntimeException("Failed to write all the bytes: " + bytes.length + " != " + result);
        }
    }

    private int writeMappedOffset(long pos, byte[] bytes, int offset) {
        FilePage filePage = pageCache.getPage(filePath, pos);

        int bytesWritten;
        bytesWritten = filePage.put(pageCache.pagePosition(pos), bytes, offset);

        if (bytesWritten < (bytes.length - offset)){
            bytesWritten += writeMappedOffset(pos + bytesWritten, bytes, offset + bytesWritten);
        }
        return bytesWritten;
    }

    int readMappedInt(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[4];
        readMapped(pos, buf);
        return Ints.fromByteArray(buf);
    }

    long readMappedLong(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[8];
        readMapped(pos, buf);
        return Longs.fromByteArray(buf);
    }

    void readMapped(long pos, byte[] buf){
        if (buf.length == 0) return;
        final int result = readMappedOffset(pos, buf, 0);
        if (result != buf.length) {
            throw new RuntimeException("FOo");
        }
    }

    private int readMappedOffset(long pos, byte[] buf, int offset) {
        FilePage filePage = pageCache.getPage(filePath, pos);

        int bytesRead;
        bytesRead = filePage.get(pageCache.pagePosition(pos), buf, offset);

        if (bytesRead < (buf.length - offset)){
            bytesRead += readMappedOffset(pos + bytesRead, buf, offset + bytesRead);
        }
        return bytesRead;
    }

    @Override
    public void flush() throws IOException {
        posBuf.force();
    }
}
