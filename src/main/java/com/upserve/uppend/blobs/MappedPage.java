package com.upserve.uppend.blobs;

import java.nio.*;

/**
 * Mapped Byte Buffer backed implementation of Page
 */
public class MappedPage implements Page {
    private final MappedByteBuffer buffer;
    private final int pageSize;
    private final int startingPosition;

    /**
     * Constructor for a MappedPage
     *
     * @param buffer the mapped byte buffer representing a page of a file
     * @param startingPosition the starting offset in a larger buffer
     * @param pageSize the size of the page to create
     */
    public MappedPage(MappedByteBuffer buffer, int startingPosition, int pageSize) {
        this.pageSize = pageSize;
        this.buffer = buffer;
        this.startingPosition = startingPosition;
    }

    /**
     * Constructor for a MappedPage
     *
     * @param buffer the mapped byte buffer representing a page of a file
     */
    public MappedPage(MappedByteBuffer buffer) {
        this(buffer, 0, buffer.capacity());
    }

    @Override
    public int get(int pagePosition, byte[] dst, int bufferOffset) {
        final int actualRead = actualOperationSize(pagePosition, pageSize, bufferOffset, dst.length);

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();
        localBuffer.position(pagePosition + startingPosition);
        localBuffer.get(dst, bufferOffset, actualRead);

        return actualRead;
    }

    @Override
    public int put(int pagePosition, byte[] src, int bufferOffset) {
        final int actualWrite = actualOperationSize(pagePosition, pageSize, bufferOffset, src.length);

        // Make a local buffer with local position
        ByteBuffer localBuffer = buffer.duplicate();
        localBuffer.position(pagePosition + startingPosition);
        localBuffer.put(src, bufferOffset, actualWrite);

        return actualWrite;
    }
}
