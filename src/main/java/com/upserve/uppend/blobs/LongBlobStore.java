package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

public class LongBlobStore extends PageMappedFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public LongBlobStore(Path file, PageCache pageCache) {
        super(file, pageCache);
    }

    public long append(long val, byte[] bytes) {
        int writeSize = recordSize(bytes);
        final long pos = appendPosition(writeSize);

        writeMapped(pos, byteRecord(val, bytes));
        return pos;
    }

    public void writeLong(long pos, long val){
        writeMappedLong(pos + 4, val);
    }

    public long readLong(long pos) {
        return readMappedLong(pos+4);
    }

    public byte[] readBlob(long pos) {
        int size = readMappedInt(pos);
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
