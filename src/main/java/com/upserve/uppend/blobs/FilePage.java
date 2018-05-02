package com.upserve.uppend.blobs;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import static java.lang.Integer.min;

/**
 * Mapped Byte Buffer backed implementation of Page
 */
public class FilePage implements Page {

    private final FileChannel channel;
    private final int pageSize;
    private final long pageStart;

    /**
     * Constructor for a FilePage
\     */
    public FilePage(FileChannel channel, long pageStart, int pageSize) {
       this.channel = channel;
       this.pageStart = pageStart;
       this.pageSize = pageSize;

    }

    @Override
    public int get(int pagePosition, byte[] dst, int bufferOffset) {
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = pageSize - pagePosition;

        final int actualRead = min(desiredRead, availableToRead);

        // Make a local buffer with local position
        ByteBuffer byteBuffer = ByteBuffer.wrap(dst, bufferOffset, actualRead);

        final int channelRead;
        try {
            channelRead = channel.read(byteBuffer, pageStart + pagePosition);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read from page", e);
        }

        if (channelRead != actualRead) throw new IllegalStateException("Failed to read past end of file");

        return  actualRead;
    }

    @Override
    public int put(int pagePosition, byte[] src, int bufferOffset) {
        final int desiredWrite = src.length - bufferOffset;
        final int availableToWrite = pageSize - pagePosition;
        final int actualWrite = min(desiredWrite, availableToWrite);

        // Make a local buffer with local position
        ByteBuffer byteBuffer = ByteBuffer.wrap(src, bufferOffset, actualWrite);

        final int channelWrite;
        try {
            channelWrite = channel.write(byteBuffer, pageStart + pagePosition);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to page", e);
        }

        if (channelWrite != actualWrite) throw new IllegalStateException("Failed to write all bytes to file page");

        return actualWrite;
    }

}
