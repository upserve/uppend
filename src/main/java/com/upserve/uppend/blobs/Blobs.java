package com.upserve.uppend.blobs;

import com.google.common.primitives.Ints;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;

public class Blobs extends PageMappedFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public Blobs(Path file, PagedFileMapper pagedFileMapper) {
        super(file, pagedFileMapper);
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos;
        pos = position.getAndAdd(writeSize);
        try {
            ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
            intBuf.putInt(bytes.length).flip();
            // File system will efficiently combine these write operations
            try {
                FileChannel chan = fileCache.getFileChannel(filePath);
                chan.write(intBuf, pos);
                chan.write(ByteBuffer.wrap(bytes), pos + 4);
            } catch (ClosedChannelException e) {
                log.warn("Blob file was closed {} - retrying!", filePath, e);
                fileCache.getFileChannel(filePath).write(intBuf, pos);
                fileCache.getFileChannel(filePath).write(ByteBuffer.wrap(bytes), pos + 4);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + filePath, e);
        }
        if (log.isTraceEnabled()) log.trace("appended {} bytes to {} at pos {}", bytes.length, filePath, pos);
        return pos;
    }

    public byte[] read(long pos) {
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", filePath, pos);
        int size = readMappedInt(pos);
        byte[] buf = new byte[size];
        readMapped(pos + 4, buf);
        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, filePath, pos);
        return buf;
    }
}
