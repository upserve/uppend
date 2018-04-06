package com.upserve.uppend.blobs;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.function.Supplier;

public class BlockedLongPairs extends PageMappedFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Supplier<ByteBuffer> recordBufSupplier = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(16);

    public BlockedLongPairs(Path file, PagedFileMapper pagedFileMapper) {
        super(file, pagedFileMapper);

        if ((position.get() % 16) != 0) {
            throw new RuntimeException("File length " + position.get() + " should be divisible by 16");
        }
    }

    /**
     * Appends a pair of long values as a block and returns the index of the block
     * @param val1 a long
     * @param val2 a long
     * @return the index of the block (file length / 16)
     */
    public long append(long val1, long val2){
        if (fileCache.readOnly()) throw new RuntimeException("Can not append value to a read only file " + filePath);
        ByteBuffer buf = recordBufSupplier.get();
        buf.putLong(val1);
        buf.putLong(val2);
        buf.flip();
        final long pos = position.getAndAdd(16);
        try {
            fileCache.getFileChannel(filePath).write(buf, pos);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to append record to BlockedLongPairs: " + filePath, e);
        }
        return pos / 16;
    }

    /**
     * Write a long value to the left long a this index
     * @param index the index at which to write the long value
     * @param val a long to be written
     */
    public void writeLeft(long index, long val){
        write(index * 16, val);
    }

    /**
     * Write a long value to the right long a this index
     * @param index the index at which to write the long value
     * @param val a long to be written
     */
    public void writeRight(long index, long val){
        write(index * 16 + 8, val);
    }

    private void write(long pos, long val){
        if (fileCache.readOnly()) throw new RuntimeException("Can not append value to a read only file " + filePath);
        ByteBuffer buf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
        buf.putLong(val);
        buf.flip();
        try {
            fileCache.getFileChannel(filePath).write(buf, pos);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to append record to BlockedLongPairs: " + filePath, e);
        }
    }

    /**
     * Get the left value of the pair at the index
     * @param index the index into the block of long pairs
     * @return the left long value at that index
     */
    public long getLeft(long index){
        return readMappedLong(index * 16);
    }

    /**
     * Get the right value of the pair at the index
     * @param index the index into the block of long pairs
     * @return the right long value at that index
     */
    public long getRight(long index){
        return readMappedLong(index * 16 + 8);
    }
}
