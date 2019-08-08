package com.upserve.uppend.blobs;

import com.google.common.collect.Maps;
import com.upserve.uppend.metrics.LongBlobStoreMetrics;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.*;

/**
 * For storing a Long position and an associated Key as a blob together.
 * The blobs are append only, but the long value can be updated.
 */
public class VirtualLongBlobStore extends VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LongBlobStoreMetrics.Adders longBlobStoreMetricsAdders;

    public VirtualLongBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile, LongBlobStoreMetrics.Adders longBlobStoreMetricsAdders) {
        super(virtualFileNumber, virtualPageFile);
        this.longBlobStoreMetricsAdders = longBlobStoreMetricsAdders;
    }

    public VirtualLongBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        this(virtualFileNumber, virtualPageFile, new LongBlobStoreMetrics.Adders());
    }

    public long append(long val, byte[] bytes) {
        final long tic = System.nanoTime();
        final int size = recordSize(bytes);
        // Ensures that the long value is aligned with a single page.
        final long pos = appendPageAlignedPosition(size, 4, 12);
        write(pos, byteRecord(val, bytes));

        longBlobStoreMetricsAdders.appendCounter.increment();
        longBlobStoreMetricsAdders.bytesAppended.add(size);
        longBlobStoreMetricsAdders.appendTimer.add(System.nanoTime() - tic);
        return pos;
    }

    public long getPosition() {
        return super.getPosition();
    }

    public void writeLong(long pos, long val) {
        final long tic = System.nanoTime();
        super.writeLong(pos + 4, val);
        longBlobStoreMetricsAdders.longWrites.increment();
        longBlobStoreMetricsAdders.longWriteTimer.add(System.nanoTime() - tic);
    }

    public long readLong(long pos) {
        final long tic = System.nanoTime();
        final long result = super.readLong(pos + 4);
        longBlobStoreMetricsAdders.longReads.increment();
        longBlobStoreMetricsAdders.longReadTimer.add(System.nanoTime() - tic);
        return result;
    }

    public byte[] readBlob(long pos) {
        final long tic = System.nanoTime();
        int size = readInt(pos);
        byte[] buf = new byte[size];
        read(pos + 12, buf);

        longBlobStoreMetricsAdders.readCounter.increment();
        longBlobStoreMetricsAdders.bytesRead.add(recordSize(buf));
        longBlobStoreMetricsAdders.readTimer.add(System.nanoTime() - tic);
        return buf;
    }

    public Stream<Map.Entry<Long, byte[]>> positionBlobStream() {
        Iterator<Map.Entry<Long, byte[]>> positionBlobIterator = positionBlobIterator();
        Spliterator<Map.Entry<Long, byte[]>> spliter = Spliterators.spliteratorUnknownSize(
                positionBlobIterator,
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, false);
    }

    private Iterator<Map.Entry<Long, byte[]>> positionBlobIterator() {
        long lastPosition = getPosition() - 1;
        return new Iterator<>() {
            long position = 0;

            @Override
            public boolean hasNext() {
                return position < lastPosition;
            }

            @Override
            public Map.Entry<Long, byte[]> next() {
                byte[] blob = readBlob(position);
                long blobPosition = position;
                position = nextAlignedPosition(blobPosition + recordSize(blob), 4, 12);

                return Maps.immutableEntry(blobPosition, blob);
            }
        };
    }

    private static int recordSize(byte[] inputBytes) {
        return inputBytes.length + 12;
    }

    private static byte[] byteRecord(long val, byte[] inputBytes) {
        byte[] result = new byte[recordSize(inputBytes)];
        System.arraycopy(int2bytes(inputBytes.length), 0, result, 0, 4);
        System.arraycopy(long2bytes(val), 0, result, 4, 8);
        System.arraycopy(inputBytes, 0, result, 12, inputBytes.length);
        return result;
    }
}
