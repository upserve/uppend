package com.upserve.uppend.blobs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.Integer.min;

/**
 * File backed implementation of Page
 */
public class FilePage implements Page {

    private final FileChannel channel;
    private final int pageSize;
    private final long pageStart;

    /**
     * Constructor for a file channel backed page
     * @param channel the open file channel
     * @param pageStart the start of the page
     * @param pageSize the page size
     */
    FilePage(FileChannel channel, long pageStart, int pageSize) {
        this.channel = channel;
        this.pageStart = pageStart;
        this.pageSize = pageSize;

    }

    @Override
    public int get(int pagePosition, byte[] dst, int bufferOffset) {
        final int actualRead = actualOperationSize(pagePosition, pageSize, bufferOffset, dst.length);

        // Make a local buffer with local position
        ByteBuffer byteBuffer = ByteBuffer.wrap(dst, bufferOffset, actualRead);

        final int channelRead;
        try {
            channelRead = channel.read(byteBuffer, pageStart + pagePosition);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read from page", e);
        }

        if (channelRead != actualRead) throw new IllegalStateException("Failed to read past end of file");

        return actualRead;
    }

    @Override
    public int put(int pagePosition, byte[] src, int bufferOffset) {
        final int actualWrite = actualOperationSize(pagePosition, pageSize, bufferOffset, src.length);

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