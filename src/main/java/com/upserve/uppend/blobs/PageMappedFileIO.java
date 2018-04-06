package com.upserve.uppend.blobs;

import com.google.common.primitives.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;

public class PageMappedFileIO  implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final Path filePath;
    final PagedFileMapper pagedFileMapper;

    final FileCache fileCache;
    final AtomicLong position;

    PageMappedFileIO(Path file, PagedFileMapper pagedFileMapper) {
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
            position = new AtomicLong(Long.MIN_VALUE);
        } else {
            try {
                position = new AtomicLong(fileCache.getFileChannel(filePath).size());
            } catch (IOException e) {
                throw new UncheckedIOException("unable to init blob filePath: " + file, e);
            }
        }
    }

    public void clear() {
        log.trace("clearing {}", filePath);
        try {
            fileCache.getFileChannel(filePath).truncate(0);
            position.set(0);
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
}
