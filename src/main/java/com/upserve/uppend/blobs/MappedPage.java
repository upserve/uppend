package com.upserve.uppend.blobs;

import java.nio.*;

import static java.lang.Integer.min;

/**
 * {@link ByteBuffer} backed implementation of {@link Page}
 * The buffer used in the constructor must be thread local
 * Buffer are not thread safe!
 */
public class MappedPage implements Page {
    private final ByteBuffer buffer;
    private final int pageSize;
    private final int startingPosition;

    /**
     * Constructor for a MappedPage
     *
     * @param buffer a {@link ThreadLocal} {@link ByteBuffer} (mapped from a file) containing a {@link Page} of a {@link VirtualPageFile}
     * @param startingPosition the starting offset in a larger buffer
     * @param pageSize the size of the page to create
     */
    public MappedPage(ByteBuffer buffer, int startingPosition, int pageSize) {
        this.pageSize = pageSize;
        this.buffer = buffer;
        this.startingPosition = startingPosition;
    }

    /**
     * Constructor for a MappedPage
     *
     * @param buffer the mapped byte buffer representing a page of a file
     */
    public MappedPage(ByteBuffer buffer) {
        this(buffer, 0, buffer.capacity());
    }

    @Override
    public int get(int pagePosition, byte[] dst, int bufferOffset) {
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = pageSize - pagePosition;

        final int actualRead = min(desiredRead, availableToRead);

        // Make a local buffer with local position
        buffer.position(pagePosition + startingPosition);
        buffer.get(dst, bufferOffset, actualRead);

        return actualRead;
    }

    @Override
    public int put(int pagePosition, byte[] src, int bufferOffset) {
        final int desiredWrite = src.length - bufferOffset;
        final int availableToWrite = pageSize - pagePosition;
        final int actualWrite = min(desiredWrite, availableToWrite);

        // Make a local buffer with local position
        buffer.position(pagePosition + startingPosition);
        buffer.put(src, bufferOffset, actualWrite);

        return actualWrite;
    }
}
