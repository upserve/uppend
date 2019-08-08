package com.upserve.uppend;

import com.upserve.uppend.metrics.*;

public class AppendOnlyStoreBuilder extends FileStoreBuilder<AppendOnlyStoreBuilder> {
    // Blocked Longs Config Options
    public static final int DEFAULT_BLOBS_PER_BLOCK = 127;

    private int blobsPerBlock = DEFAULT_BLOBS_PER_BLOCK;

    // Blob Cache Options
    public static final int DEFAULT_BLOB_PAGE_SIZE = 4 * 1024 * 1024;

    private int blobPageSize = DEFAULT_BLOB_PAGE_SIZE;

    private BlobStoreMetrics.Adders blobStoreMetricsAdders = new BlobStoreMetrics.Adders();
    private BlockedLongMetrics.Adders blockedLongMetricsAdders = new BlockedLongMetrics.Adders();

    // Blocked Long Options
    public AppendOnlyStoreBuilder withBlobsPerBlock(int blobsPerBlock) {
        this.blobsPerBlock = blobsPerBlock;
        return this;
    }

    // Blob Options
    public AppendOnlyStoreBuilder withBlobPageSize(int blobPageSize) {
        this.blobPageSize = blobPageSize;
        return this;
    }

    public AppendOnlyStore build() {
        return build(false);
    }

    public AppendOnlyStore build(boolean readOnly) {
        AppendOnlyStore store = new FileAppendOnlyStore(readOnly, this);
        if (isStoreMetrics()) store = new AppendOnlyStoreWithMetrics(store, getStoreMetricsRegistry(), getMetricsRootName());
        return store;
    }

    public ReadOnlyAppendOnlyStore buildReadOnly() {
        return build(true);
    }

    public int getBlobsPerBlock() {
        return blobsPerBlock;
    }

    public int getBlobPageSize() {
        return blobPageSize;
    }

    public BlobStoreMetrics.Adders getBlobStoreMetricsAdders() { return blobStoreMetricsAdders; }

    public BlockedLongMetrics.Adders getBlockedLongMetricsAdders() { return blockedLongMetricsAdders; }

    @Override
    public String toString() {
        return "AppendOnlyStoreBuilder{" +
                "blobsPerBlock=" + blobsPerBlock +
                ", blobPageSize=" + blobPageSize +
                '}' + super.toString();
    }
}
