package com.upserve.uppend.blobs;

import org.slf4j.Logger;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

public class VirtualBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public VirtualBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        super(virtualFileNumber, virtualPageFile);
    }

    public void write(byte[] bytes, long position) {
        // TODO Need to create a LockedPage for this to work across multiple threads / JVM
        writeMapped(position, byteRecord(bytes));
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos = appendPosition(writeSize);
        writeMapped(pos, byteRecord(bytes));
        if (log.isTraceEnabled()) log.trace("appended {} bytes to {} at pos {}", bytes.length, virtualFileNumber, pos);
        return pos;
    }

    public byte[] read(long pos) {
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", virtualFileNumber, pos);
        int size = readMappedInt(pos);
        byte[] buf = new byte[size];
        readMapped(pos + 4, buf);

        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, virtualFileNumber, pos);
        return buf;
    }

    public static byte[] byteRecord(byte[] inputBytes){
        byte[] result = new byte[inputBytes.length + 4];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(inputBytes, 0, result, 4, inputBytes.length);
        return result;
    }

}
