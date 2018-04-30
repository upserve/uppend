package com.upserve.uppend.blobs;

import com.google.common.hash.*;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

public class VirtualMutableBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final HashFunction hashFunction = Hashing.murmur3_32();

    public VirtualMutableBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        super(virtualFileNumber, virtualPageFile);
    }

    public void write(byte[] bytes, long position) {
        // TODO Need to create a LockedPage for this to work across multiple threads / JVM
        write(position, byteRecord(bytes));
    }

    public byte[] read(long pos) {
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", virtualFileNumber, pos);
        int size = readInt(pos);
        byte[] buf = new byte[size];

        byte[] checksum = new byte[4];
        readMapped(pos + 4, checksum);

        readMapped(pos + 8, buf);

        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, virtualFileNumber, pos);
        if (Arrays.equals(checksum, hashFunction.hashBytes(buf).asBytes())) {
            return buf;
        } else {
            throw new IllegalStateException("Checksum did not match for the requested blob");
        }

    }

    public static int recordSize(byte[] inputBytes){
        return inputBytes.length + 8;
    }

    public static byte[] byteRecord(byte[] inputBytes){
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(hashFunction.hashBytes(inputBytes).asBytes(), 0, result, 4, 4);
        System.arraycopy(inputBytes, 0, result, 8, inputBytes.length);

        return result;
    }
}
