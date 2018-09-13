package com.upserve.uppend.blobs;

import java.nio.*;

import static java.lang.Integer.min;

/**
 * Mapped Byte Buffer backed implementation of Page
 */
public class MappedPage implements Page {

    private final MappedByteBuffer buffer;
    private final int pageSize;

    /**
     * Constructor for a MappedPage
     *
     * @param buffer the mapped byte buffer representing a page of a file
     */
    public MappedPage(MappedByteBuffer buffer) {
        this.pageSize = buffer.capacity();
        this.buffer = buffer;
    }

    @Override
    public int get(int pagePosition, byte[] dst, int bufferOffset) {
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = pageSize - pagePosition;

        final int actualRead = min(desiredRead, availableToRead);

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();
        localBuffer.position(pagePosition);
        localBuffer.get(dst, bufferOffset, actualRead);

        return actualRead;
    }

    @Override
    public int put(int pagePosition, byte[] src, int bufferOffset) {
        final int desiredWrite = src.length - bufferOffset;
        final int availableToWrite = pageSize - pagePosition;
        final int actualWrite = min(desiredWrite, availableToWrite);

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();
        localBuffer.position(pagePosition);
        localBuffer.put(src, bufferOffset, actualWrite);

        return actualWrite;
    }

}
