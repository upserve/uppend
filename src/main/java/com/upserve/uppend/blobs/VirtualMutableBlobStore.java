package com.upserve.uppend.blobs;

import com.google.common.hash.*;
import com.upserve.uppend.metrics.MutableBlobStoreMetrics;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

public class VirtualMutableBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final HashFunction hashFunction = Hashing.murmur3_32();

    private final MutableBlobStoreMetrics.Adders mutableBlobStoreMetricsAdders;

    public VirtualMutableBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        this(virtualFileNumber, virtualPageFile, new MutableBlobStoreMetrics.Adders());
    }

    public VirtualMutableBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile, MutableBlobStoreMetrics.Adders mutableBlobStoreMetricsAdders) {
        super(virtualFileNumber, virtualPageFile);
        this.mutableBlobStoreMetricsAdders = mutableBlobStoreMetricsAdders;
    }

    public void write(long position, byte[] bytes) {
        final long tic = System.nanoTime();
        final int size = recordSize(bytes);
        super.write(position, byteRecord(bytes));
        mutableBlobStoreMetricsAdders.writeCounter.increment();
        mutableBlobStoreMetricsAdders.bytesWritten.add(size);
        mutableBlobStoreMetricsAdders.writeTimer.add(System.nanoTime() - tic);
    }

    public boolean isPageAllocated(long position) {
        return super.isPageAllocated(position);
    }

    public byte[] readChecksum(long pos){
        byte[] checksum = new byte[4];
        read(pos + 4, checksum);
        return checksum;
    }

    public byte[] read(long pos) {
        final long tic = System.nanoTime();
        if (log.isTraceEnabled()) log.trace("read mapped from  {} @ {}", virtualFileNumber, pos);
        int size = readInt(pos);
        byte[] buf = new byte[size];

        byte[] checksum = new byte[4];
        read(pos + 4, checksum);

        read(pos + 8, buf);

        if (log.isTraceEnabled()) log.trace("read mapped {} bytes from {} @ {}", size, virtualFileNumber, pos);
        if (Arrays.equals(checksum, hashFunction.hashBytes(buf).asBytes())) {
            mutableBlobStoreMetricsAdders.bytesRead.add(recordSize(buf));
            mutableBlobStoreMetricsAdders.readCounter.increment();
            mutableBlobStoreMetricsAdders.readTimer.add(System.nanoTime() - tic);
            return buf;
        } else {
            log.warn("Read at {} got size {}, checksum {} did not match bytes starting with {} (upto first 10)",
                    pos, size, checksum,  Arrays.copyOfRange(buf, 0, 10 <= size ? 10 : size ));
            throw new IllegalStateException("Checksum did not match for the requested blob");
        }
    }

    private static int recordSize(byte[] inputBytes) {
        return inputBytes.length + 8;
    }

    private static byte[] byteChecksum(byte[] inputBytes) {
        return hashFunction.hashBytes(inputBytes).asBytes();
    }

    private static byte[] byteRecord(byte[] inputBytes) {
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(byteChecksum(inputBytes), 0, result, 4, 4);
        System.arraycopy(inputBytes, 0, result, 8, inputBytes.length);

        return result;
    }
}
