package com.upserve.uppend.blobs;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.function.Supplier;

public class BlockedLongPairs extends PageMappedFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Supplier<ByteBuffer> recordBufSupplier = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(16);

    public BlockedLongPairs(Path file, PageCache pageCache) {
        super(file, pageCache);
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
        final long pos = appendPosition(16);
        writeMapped(pos, buf.array());
        return (pos - 8) / 16;
    }

    public int getMaxIndex(){
        return (int) ((getPosition() - 8) / 16);
    }

    /**
     * Write a long value to the left long a this index
     * @param index the index at which to write the long value
     * @param val a long to be written
     */
    public void writeLeft(long index, long val){
        write(index * 16 + 8 , val);
    }

    /**
     * Write a long value to the right long a this index
     * @param index the index at which to write the long value
     * @param val a long to be written
     */
    public void writeRight(long index, long val){
        write((index +1) * 16 , val);
    }

    private void write(long pos, long val){
        if (fileCache.readOnly()) throw new RuntimeException("Can not append value to a read only file " + filePath);
        writeMappedLong(pos, val);
    }

    /**
     * Get the left value of the pair at the index
     * @param index the index into the block of long pairs
     * @return the left long value at that index
     */
    public long getLeft(long index){
        return readMappedLong(index * 16 + 8);
    }

    /**
     * Get the right value of the pair at the index
     * @param index the index into the block of long pairs
     * @return the right long value at that index
     */
    public long getRight(long index){
        return readMappedLong((index + 1) * 16);
    }
}
