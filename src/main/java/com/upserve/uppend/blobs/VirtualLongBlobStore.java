package com.upserve.uppend.blobs;

import com.google.common.collect.Maps;
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

    public VirtualLongBlobStore(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        super(virtualFileNumber, virtualPageFile);
    }

    public long append(long val, byte[] bytes) {
        // Ensures that the long value is aligned with a single page.
        final long pos = appendPageAlignedPosition(recordSize(bytes), 4, 12);

        write(pos, byteRecord(val, bytes));
        return pos;
    }

    public long getPosition() {
        return super.getPosition();
    }

    public void writeLong(long pos, long val) {
        super.writeLong(pos + 4, val);
    }

    public long readLong(long pos) {
        return super.readLong(pos + 4);
    }

    public byte[] readBlob(long pos) {
        int size = readInt(pos);
        byte[] buf = new byte[size];
        read(pos + 12, buf);

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
