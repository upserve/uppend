package com.upserve.uppend.blobs;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

public class VirtualAppendOnlyBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public VirtualAppendOnlyBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        super(virtualFileNumber, virtualPageFile);
    }

    public long append(byte[] bytes) {
        final long pos = appendPosition(recordSize(bytes));
        write(pos, byteRecord(bytes));
        if (log.isTraceEnabled()) log.trace("appended {} bytes to {} at pos {}", bytes.length, virtualFileNumber, pos);
        return pos;
    }

    public long getPosition() {
        return super.getPosition();
    }

    public byte[] read(long pos) {
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", virtualFileNumber, pos);
        int size = readInt(pos);
        byte[] buf = new byte[size];
        super.read(pos + 4, buf);

        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, virtualFileNumber, pos);
        return buf;
    }

    public static int recordSize(byte[] inputBytes) {
        return inputBytes.length + 4;
    }

    public static byte[] byteRecord(byte[] inputBytes) {
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(inputBytes, 0, result, 4, inputBytes.length);

        return result;
    }
}
