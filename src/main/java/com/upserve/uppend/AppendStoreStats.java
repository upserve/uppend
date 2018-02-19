package com.upserve.uppend;

public class AppendStoreStats {
    private static final long GB = 1024*1024*1024;

    final long blobBytes;
    final long blockBytes;
    final long writeCacheSize;
    final long writeCacheKeys;
    final long bufferSize;
    final long bufferEntries;
    final long bufferTasks;
    final long writeCacheTasks;

    public AppendStoreStats(long blobBytes, long blockBytes, long writeCacheSize, long writeCacheKeys, long writeCacheTasks, long bufferSize, long bufferEntries, long bufferTasks){
        this.blobBytes = blobBytes;
        this.blockBytes = blockBytes;
        this.writeCacheSize = writeCacheSize;
        this.writeCacheKeys = writeCacheKeys;
        this.writeCacheTasks = writeCacheTasks;
        this.bufferSize = bufferSize;
        this.bufferEntries = bufferEntries;
        this.bufferTasks = bufferTasks;
    }

    @Override
    public String toString() {
        return String.format("Blobs: %.2f(gb); Blocks: %.2f(gb); WriteCache - Size: %6d, Keys: %8d, Tasks: %6d; WriteBuffer - Size: %6d, Entries: %8d, Tasks: %6d",
                blobBytes / (double) GB,
                blockBytes / (double) GB,
                writeCacheSize,
                writeCacheKeys,
                writeCacheTasks,
                bufferSize,
                bufferEntries,
                bufferTasks
                );
    }


}
