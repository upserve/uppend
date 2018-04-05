package com.upserve.uppend.blobs;

import com.google.common.primitives.Ints;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path filePath;
    private final PagedFileMapper pagedFileMapper;

    private final FileCache fileCache;
    private final AtomicLong blobPosition;

    public Blobs(Path file, PagedFileMapper pagedFileMapper) {
        this.filePath = file;
        this.pagedFileMapper = pagedFileMapper;

        this.fileCache = pagedFileMapper.getFileCache();

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        if (fileCache.readOnly()) {
            blobPosition = new AtomicLong(Long.MIN_VALUE);
        } else {
            try {
                blobPosition = new AtomicLong(fileCache.getFileChannel(filePath).size());
            } catch (IOException e) {
                throw new UncheckedIOException("unable to init blob filePath: " + file, e);
            }
        }
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos;
        pos = blobPosition.getAndAdd(writeSize);
        try {
            ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
            intBuf.putInt(bytes.length).flip();
            // File system will efficiently combine these write operations
            fileCache.getFileChannel(filePath).write(intBuf, pos);
            fileCache.getFileChannel(filePath).write(ByteBuffer.wrap(bytes), pos + 4);
        } catch (IOException e) {
            throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + filePath, e);
        }
        log.trace("appended {} bytes to {} at pos {}", bytes.length, filePath, pos);
        return pos;
    }

    public byte[] read(long pos) {
        log.trace("read mapped from  {} @ {}", filePath, pos);
        int size = readMappedInt(pos);
        byte[] buf = new byte[size];
        readMapped(pos + 4, buf);
        log.trace("read mapped {} bytes from {} @ {}", size, filePath, pos);
        return buf;
    }

    public void clear() {
        log.trace("clearing {}", filePath);
        try {
            fileCache.getFileChannel(filePath).truncate(0);
            blobPosition.set(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() {
        log.trace("NO-OP CLOSE {}", filePath);
    }

    @Override
    public void flush() {
        try {
            fileCache.getFileChannel(filePath).force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to flush: " + filePath, e);
        }
    }

    private int readMappedInt(long pos) {
        byte[] buf = new byte[4];
        readMapped(pos, buf);
        return Ints.fromByteArray(buf);
    }

    private void readMapped(long pos, byte[] buf){
        final int result = readMappedOffset(pos, buf, 0);
        if (result != buf.length) {
            throw new RuntimeException("FOo");
        }
    }

    private int readMappedOffset(long pos, byte[] buf, int offset) {
        FilePage filePage = pagedFileMapper.getPage(filePath, pos);

        int bytesRead;
        try {
            bytesRead = filePage.get(pos, buf, offset);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read bytes in blob store", e);
        }

        if (bytesRead < buf.length){
            bytesRead += readMappedOffset(pos + bytesRead, buf, offset + bytesRead);
        }
        return bytesRead;
    }

    public static byte[] read(FileChannel chan, long pos) {
        log.trace("reading @ {}", pos);
        ByteBuffer intBuffer = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        try {
            chan.read(intBuffer, pos);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read 4 bytes at pos " + pos, e);
        }
        intBuffer.flip();
        int size = intBuffer.getInt();
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int len = buf.remaining();
        try {
            chan.read(buf, pos + 4);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read " + len + " bytes at pos " + pos, e);
        }
        log.trace("read {} bytes @ {}", size, pos);
        return bytes;
    }
}
