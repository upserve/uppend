package com.upserve.uppend.blobs;


import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

/**
 * For storing a Long position and an associated Key as a blob together.
 * The blobs are append only, but the long value can be updated.
 * TODO address non atomic nature of the long update
 */
public class VirtualLongBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public VirtualLongBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        super(virtualFileNumber, virtualPageFile);
    }

    public long append(long val, byte[] bytes) {
        int writeSize = recordSize(bytes);
        final long pos = appendPosition(writeSize);

        write(pos, byteRecord(val, bytes));
        return pos;
    }

    public long getPosition(){
        return super.getPosition();
    }

    public void writeLong(long pos, long val){
        super.writeLong(pos + 4, val);
    }

    public long readLong(long pos) {
        return super.readLong(pos+4);
    }

    public byte[] readBlob(long pos) {
        int size = readInt(pos);
        byte[] buf = new byte[size];
        readMapped(pos + 12, buf);

        return buf;
    }

    public static int recordSize(byte[] inputBytes) {
        return inputBytes.length + 12;
    }

    public static byte[] byteRecord(long val, byte[] inputBytes){
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(long2bytes(val), 0, result, 4, 8);
        System.arraycopy(inputBytes, 0, result, 12, inputBytes.length);
        return result;
    }
}
