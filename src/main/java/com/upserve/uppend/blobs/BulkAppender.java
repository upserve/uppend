package com.upserve.uppend.blobs;

import java.util.concurrent.atomic.AtomicLong;

public class BulkAppender {

    private final AtomicLong filePosition;
    private final long initialPosition;
    private final VirtualPageFileIO virtualPageFileIO;

    private final byte[] bulkBytes;


    public BulkAppender(VirtualPageFileIO virtualPageFileIO, int bulkWriteSize) {
        filePosition = new AtomicLong(virtualPageFileIO.getPosition());
        initialPosition = filePosition.get();

        this.virtualPageFileIO = virtualPageFileIO;

        bulkBytes = new byte[bulkWriteSize];
    }

    public long getBulkAppendPosition(long size) {
        return filePosition.getAndAdd(size);
    }

    public void addBulkAppendBytes(long pos, byte[] bytes) {
        System.arraycopy(bytes,0, bulkBytes, (int) (pos - initialPosition), bytes.length);
    }

    public void finishBulkAppend() {
        final long writePosition = virtualPageFileIO.appendPosition(bulkBytes.length);
        if (writePosition + bulkBytes.length != filePosition.get()) {
            throw new IllegalStateException("Bulk appender position and length do not match");
        }

        virtualPageFileIO.write(writePosition, bulkBytes);
    }
}
