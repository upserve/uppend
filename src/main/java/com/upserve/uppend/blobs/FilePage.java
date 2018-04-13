package com.upserve.uppend.blobs;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class FilePage implements Flushable {

    private final byte[] bytes;

    private final MappedByteBuffer buffer;
    private final PageKey key;
    private final int pageSize;

    /**
     * Lazy loaded page of a file. The open file channel must be provided to read bytes but data can be cached regardless
     * @param key
     * @param pageSize
     * @throws IOException
     */
    public FilePage(PageKey key, int pageSize, FileCache fileCache) throws IOException {
        this.key = key;
        this.pageSize = pageSize;
        this.bytes = new byte[pageSize];

        long pos = (long) pageSize * (long) key.getPage();
        buffer = fileCache
                .getFileChannel(key.getFilePath())
                .map(fileCache.readOnly() ? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE, pos, pageSize);

    }

    protected int get(long filePosition, byte[] dst, int bufferOffset) {
        final int pagePos = pagePosition(filePosition);
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = pageSize - pagePos;

        final int actualRead = (availableToRead >= desiredRead) ? desiredRead : availableToRead;

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();

        localBuffer.position(pagePos);

        localBuffer.get(dst, bufferOffset, actualRead);

        return  actualRead;
    }

    protected int put(long filePosition, byte[] src, int bufferOffset) {
        final int pagePos = pagePosition(filePosition);

        final int desiredWrite = src.length - bufferOffset;
        final int availableToWrite = pageSize - pagePos;
        final int actualWrite = (availableToWrite >= desiredWrite) ? desiredWrite : availableToWrite;

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();

        localBuffer.position(pagePos);

        localBuffer.put(src, bufferOffset, actualWrite);

        return actualWrite;
    }

    private int pagePosition(long pos){
        return (int) (pos % (long) pageSize);
    }

    @Override
    public void flush() {
        buffer.force();
    }
}
