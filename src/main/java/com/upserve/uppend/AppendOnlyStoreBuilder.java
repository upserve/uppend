package com.upserve.uppend;

import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;

import java.util.concurrent.*;

public class AppendOnlyStoreBuilder extends FileStoreBuilder<AppendOnlyStoreBuilder> {
    // Blocked Longs Config Options
    public static final int DEFAULT_BLOBS_PER_BLOCK = 127;

    private int blobsPerBlock = DEFAULT_BLOBS_PER_BLOCK;

    // Blob Cache Options
    public static final int DEFAULT_BLOB_PAGE_SIZE = 4 * 1024 * 1024;

    private int blobPageSize = DEFAULT_BLOB_PAGE_SIZE;

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

    @Override
    public String toString() {
        return "AppendOnlyStoreBuilder{" +
                "blobsPerBlock=" + getBlobsPerBlock() +
                ", blobPageSize=" + getBlobPageSize() +
                ", storeName='" + getStoreName() + '\'' +
                ", partitionSize=" + getPartitionSize() +
                ", lookupHashSize=" + getLookupHashSize() +
                ", lookupPageSize=" + getLookupPageSize() +
                ", maximumLookupKeyCacheWeight=" + getMaximumLookupKeyCacheWeight() +
                ", initialLookupKeyCacheSize=" + getInitialLookupKeyCacheSize() +
                ", metadataTTL=" + getMetadataTTL() +
                ", metaDataPageSize=" + getMetadataPageSize() +
                ", lookupKeyCacheExecutorService=" + getLookupKeyCacheExecutorService() +
                ", flushDelaySeconds=" + getFlushDelaySeconds() +
                ", flushThreshold=" + getFlushThreshold() +
                ", dir=" + getDir() +
                ", storeMetricsRegistry=" + getStoreMetricsRegistry() +
                ", metricsRootName='" + getMetricsRootName() + '\'' +
                ", storeMetrics=" + isStoreMetrics() +
                ", cacheMetricsRegistry=" + getCacheMetricsRegistry() +
                ", cacheMetrics=" + isCacheMetrics() +
                '}';
    }
}
