package com.upserve.uppend.blobs;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;

public class BlobStore extends PageMappedFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public BlobStore(Path file, PageCache pageCache) {
        super(file, pageCache);
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos = appendPosition(writeSize);
        writeMapped(pos, byteRecord(bytes));
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

    public static byte[] byteRecord(byte[] inputBytes){
        byte[] result = new byte[inputBytes.length + 4];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(inputBytes, 0, result, 4, inputBytes.length);
        return result;
    }

}
