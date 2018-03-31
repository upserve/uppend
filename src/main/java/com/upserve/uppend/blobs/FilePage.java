package com.upserve.uppend.blobs;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class FilePage {

    private final byte[] bytes;
    private final PageKey key;
    private final int pageSize;

    private int bytesRead;

    public FilePage(PageKey key, int pageSize) throws IOException {
        this.key = key;
        this.pageSize = pageSize;
        this.bytes = new byte[pageSize];
        loadBytes();
    }

    protected int get(long filePosition, byte[] dst, int bufferOffset) throws IOException {
        int result = getBytes(filePosition, dst, bufferOffset);
        if (result > 0) return result;

        loadMore();
        result =  getBytes(filePosition, dst, bufferOffset);
        if (result > 0) return result;

        throw new IOException("Unable to read " + (dst.length - bufferOffset) + " bytes at pos " + filePosition + ": Past end of file: " + key.getFileName());
    }

    private int pagePosition(long pos){
        return (int) (pos % (long) pageSize);
    }

    private int getBytes(long filePosition, byte[] dst, int bufferOffset) throws IOException {
        final int pagePos = pagePosition(filePosition);
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = bytesRead - pagePos;

        final int actualRead;
        if (availableToRead >= desiredRead) {
            actualRead = desiredRead;
        } else if (bytesRead == pageSize){
            // Read to end of page
            actualRead = availableToRead;
        } else {
            return -1;
        }

        try {
            System.arraycopy(bytes,
                    pagePos,
                    dst,
                    bufferOffset,
                    actualRead);
        } catch (RuntimeException e) {
            throw new IOException("Unable to read " + desiredRead + " bytes from page " + key.getPage() + " at pos " + filePosition + " in file " + key.getFileName(), e);
        }
        return actualRead;
    }

    private void loadMore() throws IOException {
        synchronized (this) {
            if (bytesRead < pageSize){
                loadBytes();
            }
        }
    }

    private void loadBytes() throws IOException {
        long pos = (long) key.getPage() * pageSize + bytesRead;
        ByteBuffer buffer = ByteBuffer.wrap(bytes, bytesRead, pageSize - bytesRead);
        try {
            bytesRead += key.getChan().read(buffer, pos);
        } catch (IOException e) {
            throw new IOException("Unable to read page " + key.getPage() + " at pos " + pos + " with Size " + pageSize + " in file " + key.getFileName(), e);
        }
    }
}
