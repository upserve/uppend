package com.upserve.uppend;

import com.upserve.uppend.blobs.PageCache;
import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;

import java.util.concurrent.*;

public class AppendOnlyStoreBuilder extends FileStoreBuilder<AppendOnlyStoreBuilder> {

    // Blocked Longs Config Options
    public static final int DEFAULT_BLOBS_PER_BLOCK = 127;

    private int blobsPerBlock = DEFAULT_BLOBS_PER_BLOCK;

    // Blob Cache Options
    public static final int DEFAULT_BLOB_PAGE_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_MAXIMUM_CACHED_BLOB_PAGES = 1024;
    public static final int DEFAULT_INITIAL_BLOB_PAGE_CACHE_SIZE = 256;

    private int blobPageSize = DEFAULT_BLOB_PAGE_SIZE;
    private int maximumCachedBlobPages = DEFAULT_MAXIMUM_CACHED_BLOB_PAGES;
    private int initialBlobPageCacheSize = DEFAULT_INITIAL_BLOB_PAGE_CACHE_SIZE;

    private ExecutorService blobCacheExecutorService = ForkJoinPool.commonPool();

    // Blocked Long Options
    public AppendOnlyStoreBuilder withBlobsPerBlock(int blobsPerBlock) {
        this.blobsPerBlock = blobsPerBlock;
        return this;
    }

    // Blob Cache Options
    public AppendOnlyStoreBuilder withBlobPageSize(int blobPageSize) {
        this.blobPageSize = blobPageSize;
        return this;
    }

    public AppendOnlyStoreBuilder withMaximumBlobCacheSize(int maximumBlobCacheSize) {
        this.maximumCachedBlobPages = maximumBlobCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withInitialBlobCacheSize(int initialBlobCacheSize) {
        this.initialBlobPageCacheSize = initialBlobCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withBlobCacheExecutorService(ExecutorService blobCacheExecutorService) {
        this.blobCacheExecutorService = blobCacheExecutorService;
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

    public PageCache buildBlobPageCache(String metricsPrefix) {
        return new PageCache(
                getBlobPageSize(),
                getInitialBlobPageCacheSize(),
                getMaximumCachedBlobPages(),
                getBlobCacheExecutorService(),
                metricsSupplier(metricsPrefix, BLOB_PAGE_CACHE_METRICS)
        );
    }

    public static AppendOnlyStoreBuilder getDefaultTestBuilder() {
        return getDefaultTestBuilder(ForkJoinPool.commonPool());
    }

    public static AppendOnlyStoreBuilder getDefaultTestBuilder(ExecutorService testService) {
        return new AppendOnlyStoreBuilder()
                .withStoreName("test")
                .withBlobPageSize(64 * 1024)
                .withBlobsPerBlock(30)
                .withInitialBlobCacheSize(64)
                .withMaximumBlobCacheSize(128)
                .withBlobCacheExecutorService(testService)
                .withInitialLookupKeyCacheSize(64)
                .withMaximumLookupKeyCacheWeight(100 * 1024)
                .withLookupKeyCacheExecutorService(testService)
                .withInitialMetaDataCacheSize(64)
                .withMaximumMetaDataCacheWeight(100 * 1024)
                .withLookupMetaDataCacheExecutorService(testService)
                .withLongLookupHashSize(16)
                .withLookupPageSize(16 * 1024)
                .withLookupPageCacheExecutorService(testService)
                .withCacheMetrics();

    }

    public int getBlobsPerBlock() {
        return blobsPerBlock;
    }

    public int getBlobPageSize() {
        return blobPageSize;
    }

    public int getMaximumCachedBlobPages() {
        return maximumCachedBlobPages;
    }

    public int getInitialBlobPageCacheSize() {
        return initialBlobPageCacheSize;
    }

    public ExecutorService getBlobCacheExecutorService() {
        return blobCacheExecutorService;
    }

    @Override
    public String toString() {
        return "AppendOnlyStoreBuilder{" +
                "blobsPerBlock=" + blobsPerBlock +
                ", blobPageSize=" + blobPageSize +
                ", maximumCachedBlobPages=" + maximumCachedBlobPages +
                ", initialBlobPageCacheSize=" + initialBlobPageCacheSize +
                ", blobCacheExecutorService=" + blobCacheExecutorService +
                ", storeName='" + storeName + '\'' +
                ", partitionSize=" + partitionSize +
                ", lookupHashSize=" + lookupHashSize +
                ", lookupPageSize=" + lookupPageSize +
                ", initialLookupPageCacheSize=" + initialLookupPageCacheSize +
                ", maximumLookupPageCacheSize=" + maximumLookupPageCacheSize +
                ", maximumLookupKeyCacheWeight=" + maximumLookupKeyCacheWeight +
                ", initialLookupKeyCacheSize=" + initialLookupKeyCacheSize +
                ", maximumMetaDataCacheWeight=" + maximumMetaDataCacheWeight +
                ", initialMetaDataCacheSize=" + initialMetaDataCacheSize +
                ", metadataTTL=" + metadataTTL +
                ", metaDataPageSize=" + metaDataPageSize +
                ", lookupKeyCacheExecutorService=" + lookupKeyCacheExecutorService +
                ", lookupMetaDataCacheExecutorService=" + lookupMetaDataCacheExecutorService +
                ", lookupPageCacheExecutorService=" + lookupPageCacheExecutorService +
                ", flushDelaySeconds=" + flushDelaySeconds +
                ", flushThreshold=" + flushThreshold +
                ", dir=" + dir +
                ", storeMetricsRegistry=" + storeMetricsRegistry +
                ", metricsRootName='" + metricsRootName + '\'' +
                ", storeMetrics=" + storeMetrics +
                ", cacheMetricsRegistry=" + cacheMetricsRegistry +
                ", cacheMetrics=" + cacheMetrics +
                '}';
    }
}
