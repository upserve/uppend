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

    /**
     * Read a byte array at this position from the virtual blob store
     * Results are unpredictable for bad position requests. It may lead to a negative size and a NegativeArraySizeException
     * it may lead to an IllegalStateException if the page does not yet exist for that position or it may result in an
     * empty array value if the page exists but the position is currently past the end of the virtual file.
     * @param pos the position to read from in the virtual file
     * @return the byte array blob
     */
    public byte[] read(long pos) {
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", virtualFileNumber, pos);
        int size = readInt(pos);
        byte[] buf = new byte[size];
        super.read(pos + 4, buf);

        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, virtualFileNumber, pos);
        return buf;
    }

    private static int recordSize(byte[] inputBytes) {
        return inputBytes.length + 4;
    }

    private static byte[] byteRecord(byte[] inputBytes) {
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(inputBytes, 0, result, 4, inputBytes.length);

        return result;
    }
}
