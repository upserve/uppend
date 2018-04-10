package com.upserve.uppend.blobs;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class FilePage {

    private final byte[] bytes;
    private final PageKey key;
    private final int pageSize;
    private final FileCache fileCache;
    private final AtomicInteger bytesRead;

    /**
     * Lazy loaded page of a file. The open file channel must be provided to read bytes but data can be cached regardless
     * @param key
     * @param pageSize
     * @throws IOException
     */
    public FilePage(PageKey key, int pageSize, FileCache fileCache) throws IOException {
        this.key = key;
        this.pageSize = pageSize;
        this.fileCache = fileCache;
        this.bytes = new byte[pageSize];

        bytesRead = new AtomicInteger();
    }

    protected int get(long filePosition, byte[] dst, int bufferOffset) throws IOException {
        int result = getBytes(filePosition, dst, bufferOffset);
        if (result > 0) return result;

        loadMore();
        result =  getBytes(filePosition, dst, bufferOffset);
        if (result > 0) return result;

        loadMore();
        result =  getBytes(filePosition, dst, bufferOffset);
        if (result > 0) return result;

        throw new IOException("Unable to read " + (dst.length - bufferOffset) + " bytes at pos " + filePosition + ": Past end of file: " + key.getFilePath());
    }

    private int pagePosition(long pos){
        return (int) (pos % (long) pageSize);
    }

    private int getBytes(long filePosition, byte[] dst, int bufferOffset) throws IOException {
        final int pagePos = pagePosition(filePosition);
        final int desiredRead = dst.length - bufferOffset;
        final int availableToRead = bytesRead.get() - pagePos;

        final int actualRead;
        if (availableToRead >= desiredRead) {
            actualRead = desiredRead;
        } else if (bytesRead.get() == pageSize){
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
            throw new IOException("Unable to read " + desiredRead + " bytes from page " + key.getPage() + " at pos " + filePosition + " in file " + key.getFilePath(), e);
        }
        return actualRead;
    }

    private void loadMore() throws IOException {
        synchronized (this) {
            if (bytesRead.get() < pageSize){
                loadBytes();
            }
        }
    }

    private void loadBytes() throws IOException {
        long pos = (long) key.getPage() * pageSize + bytesRead.get();
        ByteBuffer buffer = ByteBuffer.wrap(bytes, bytesRead.get(), pageSize - bytesRead.get());
        try {
            bytesRead.addAndGet(fileCache.getFileChannel(key.getFilePath()).read(buffer, pos));
        } catch (IOException e) {
            throw new IOException("Unable to read page " + key.getPage() + " at pos " + pos + " with Size " + pageSize + " in file " + key.getFilePath(), e);
        }
    }
}
