package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;

import java.nio.file.Path;
import java.util.concurrent.*;

public class AppendOnlyStoreBuilder {

    // Blocked Longs Config Options
    public static final int DEFAULT_BLOBS_PER_BLOCK = 127;
    private int blobsPerBlock = DEFAULT_BLOBS_PER_BLOCK;

    // Blob Cache Options
    public static final int DEFAULT_BLOB_PAGE_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_MAXIMUM_BLOB_CACHE_SIZE = 1024;
    public static final int DEFAULT_INITIAL_BLOB_CACHE_SIZE = 256;

    private int blobPageSize = DEFAULT_BLOB_PAGE_SIZE;
    private int maximumBlobCacheSize = DEFAULT_MAXIMUM_BLOB_CACHE_SIZE;
    private int initialBlobCacheSize = DEFAULT_INITIAL_BLOB_CACHE_SIZE;

    private ExecutorService blobCacheExecutorService = ForkJoinPool.commonPool();

    // Long lookup Cache Options
    public static final int DEFAULT_LOOKUP_HASH_SIZE = 256;
    public static final int DEFAULT_LOOKUP_PAGE_SIZE = 256 * 1024;
    public static final int DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE = 1024;
    public static final int DEFAULT_MAXIMUM_LOOKUP_PAGE_CACHE_SIZE = 16 * 1024;

    public static final long DEFAULT_MAXIMUM_LOOKUP_KEY_CACHE_WEIGHT = 1_000_000;
    public static final int DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE = 1000;

    public static final long DEFAULT_MAXIMUM_METADATA_CACHE_WEIGHT = 1_000_000;
    public static final int DEFAULT_INITIAL_METADATA_CACHE_SIZE = 1000;

    private int lookupHashSize = DEFAULT_LOOKUP_HASH_SIZE;

    private int lookupPageSize = DEFAULT_LOOKUP_PAGE_SIZE;
    private int initialLookupPageCacheSize = DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE;
    private int maximumLookupPageCacheSize = DEFAULT_MAXIMUM_LOOKUP_PAGE_CACHE_SIZE;

    private long maximumLookupKeyCacheWeight = DEFAULT_MAXIMUM_LOOKUP_KEY_CACHE_WEIGHT;
    private int initialLookupKeyCacheSize = DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE;

    private long maximumMetaDataCacheWeight = DEFAULT_MAXIMUM_METADATA_CACHE_WEIGHT;
    private int initialMetaDataCacheSize = DEFAULT_INITIAL_METADATA_CACHE_SIZE;

    private ExecutorService lookupKeyCacheExecutorService = ForkJoinPool.commonPool();
    private ExecutorService lookupMetaDataCacheExecutorService = ForkJoinPool.commonPool();
    private ExecutorService lookupPageCacheExecutorService = ForkJoinPool.commonPool();

    // File Cache Options
    public static final int DEFAULT_MAXIMUM_FILE_CACHE_SIZE = 8_000;
    public static final int DEFAULT_INITIAL_FILE_CACHE_SIZE = 1000;

    private int maximumFileCacheSize = DEFAULT_MAXIMUM_FILE_CACHE_SIZE;
    private int intialFileCacheSize = DEFAULT_INITIAL_FILE_CACHE_SIZE;

    private ExecutorService fileCacheExecutorService = ForkJoinPool.commonPool();

    // Append Store Options
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    private int flushDelaySeconds = DEFAULT_FLUSH_DELAY_SECONDS;
    private Path dir;
    private MetricRegistry metrics;

    // Blocked Long Options
    public AppendOnlyStoreBuilder withBlobsPerBlock(int blobsPerBlock){
        this.blobsPerBlock = blobsPerBlock;
        return this;
    }

    // Blob Cache Options
    public AppendOnlyStoreBuilder withBlobPageSize(int blobPageSize){
        this.blobPageSize = blobPageSize;
        return this;
    }

    public AppendOnlyStoreBuilder withMaximumBlobCacheSize(int maximumBlobCacheSize){
        this.maximumBlobCacheSize = maximumBlobCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withInitialBlobCacheSize(int initialBlobCacheSize){
        this.initialBlobCacheSize = initialBlobCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withBlobCacheExecutorService(ExecutorService blobCacheExecutorService){
        this.blobCacheExecutorService = blobCacheExecutorService;
        return this;
    }

    // Long lookup Cache Options
    public AppendOnlyStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.lookupHashSize = longLookupHashSize;
        return this;
    }

    public AppendOnlyStoreBuilder withLookupPageSize(int lookupPageSize) {
        this.lookupPageSize = lookupPageSize;
        return this;
    }

    public AppendOnlyStoreBuilder withInitialLookupPageCacheSize(int initialLookupPageCacheSize) {
        this.initialLookupPageCacheSize = initialLookupPageCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withMaximumLookupPageCacheSize(int maximumLookupPageCacheSize) {
        this.maximumLookupPageCacheSize = maximumLookupPageCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withMaximumLookupKeyCacheWeight(int maximumLookupKeyCacheWeight) {
        this.maximumLookupKeyCacheWeight = maximumLookupKeyCacheWeight;
        return this;
    }

    public AppendOnlyStoreBuilder withInitialLookupKeyCacheSize(int initialLookupKeyCacheSize) {
        this.initialLookupKeyCacheSize = initialLookupKeyCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withMaximumMetaDataCacheWeight(int maximumMetaDataCacheWeight) {
        this.maximumMetaDataCacheWeight = maximumMetaDataCacheWeight;
        return this;
    }

    public AppendOnlyStoreBuilder withInitialMetaDataCacheSize(int initialMetaDataCacheSize) {
        this.initialMetaDataCacheSize = initialMetaDataCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withLookupKeyCacheExecutorService(ExecutorService lookupKeyCacheExecutorService){
        this.lookupKeyCacheExecutorService = lookupKeyCacheExecutorService;
        return this;
    }

    public AppendOnlyStoreBuilder withLookupMetaDataCacheExecutorService(ExecutorService lookupMetaDataCacheExecutorService){
        this.lookupMetaDataCacheExecutorService = lookupMetaDataCacheExecutorService;
        return this;
    }

    public AppendOnlyStoreBuilder withLookupPageCacheExecutorService(ExecutorService lookupPageCacheExecutorService){
        this.lookupPageCacheExecutorService = lookupPageCacheExecutorService;
        return this;
    }

    // File Cache Options
    public AppendOnlyStoreBuilder withMaximumFileCacheSize(int maximumFileCacheSize) {
        this.maximumFileCacheSize = maximumFileCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withIntialFileCacheSize(int intialFileCacheSize) {
        this.intialFileCacheSize = intialFileCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withFileCacheExecutorService(ExecutorService fileCacheExecutorService){
        this.fileCacheExecutorService = fileCacheExecutorService;
        return this;
    }

    // Append Store Options
    public AppendOnlyStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    public AppendOnlyStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public AppendOnlyStoreBuilder withMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
        return this;
    }

    public AppendOnlyStore build(boolean readOnly) {
        if (readOnly && flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS) throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        AppendOnlyStore store = new FileAppendOnlyStore( readOnly, this);

        if (metrics != null) {
            store = new AppendOnlyStoreWithMetrics(store, metrics);
        }
        return store;
    }

    public ReadOnlyAppendOnlyStore buildReadOnly() {
        if (flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS) throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        AppendOnlyStore store = new FileAppendOnlyStore( true, this);
        if (metrics != null) {
            store = new AppendOnlyStoreWithMetrics(store, metrics);
        }
        return store;
    }

    private static AppendOnlyStoreBuilder defaultTestBuilder = new AppendOnlyStoreBuilder()
            .withBlobPageSize(64*1024)
            .withBlobsPerBlock(30)
            .withInitialBlobCacheSize(64)
            .withMaximumBlobCacheSize(128)
            .withInitialLookupKeyCacheSize(64)
            .withMaximumLookupKeyCacheWeight(100 * 1024)
            .withInitialMetaDataCacheSize(64)
            .withMaximumMetaDataCacheWeight(100 * 1024)
            .withIntialFileCacheSize(128)
            .withMaximumFileCacheSize(1024)
            .withLongLookupHashSize(16)
            .withLookupPageSize(16*1024);

    public static AppendOnlyStoreBuilder getDefaultTestBuilder(){
        return defaultTestBuilder;
    }

    public int getBlobsPerBlock() {
        return blobsPerBlock;
    }

    public int getBlobPageSize() {
        return blobPageSize;
    }

    public int getMaximumBlobCacheSize() {
        return maximumBlobCacheSize;
    }

    public int getInitialBlobCacheSize() {
        return initialBlobCacheSize;
    }

    public ExecutorService getBlobCacheExecutorService() {
        return blobCacheExecutorService;
    }

    public int getLookupHashSize() {
        return lookupHashSize;
    }

    public int getLookupPageSize() {
        return lookupPageSize;
    }

    public int getMaximumFileCacheSize() {
        return maximumFileCacheSize;
    }

    public int getIntialFileCacheSize() {
        return intialFileCacheSize;
    }

    public ExecutorService getFileCacheExecutorService() {
        return fileCacheExecutorService;
    }

    public int getFlushDelaySeconds() {
        return flushDelaySeconds;
    }

    public Path getDir() {
        return dir;
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }

    public int getInitialLookupPageCacheSize() {
        return initialLookupPageCacheSize;
    }

    public int getMaximumLookupPageCacheSize() {
        return maximumLookupPageCacheSize;
    }

    public long getMaximumLookupKeyCacheWeight() {
        return maximumLookupKeyCacheWeight;
    }

    public int getInitialLookupKeyCacheSize() {
        return initialLookupKeyCacheSize;
    }

    public long getMaximumMetaDataCacheWeight() {
        return maximumMetaDataCacheWeight;
    }

    public int getInitialMetaDataCacheSize() {
        return initialMetaDataCacheSize;
    }

    public ExecutorService getLookupKeyCacheExecutorService() {
        return lookupKeyCacheExecutorService;
    }

    public ExecutorService getLookupMetaDataCacheExecutorService() {
        return lookupMetaDataCacheExecutorService;
    }

    public ExecutorService getLookupPageCacheExecutorService() {
        return lookupPageCacheExecutorService;
    }

    @Override
    public String toString() {
        return "AppendOnlyStoreBuilder{" +
                "blobsPerBlock=" + blobsPerBlock +
                ", blobPageSize=" + blobPageSize +
                ", maximumBlobCacheSize=" + maximumBlobCacheSize +
                ", initialBlobCacheSize=" + initialBlobCacheSize +
                ", blobCacheExecutorService=" + blobCacheExecutorService +
                ", lookupHashSize=" + lookupHashSize +
                ", lookupPageSize=" + lookupPageSize +
                ", initialLookupPageCacheSize=" + initialLookupPageCacheSize +
                ", maximumLookupPageCacheSize=" + maximumLookupPageCacheSize +
                ", maximumLookupKeyCacheWeight=" + maximumLookupKeyCacheWeight +
                ", initialLookupKeyCacheSize=" + initialLookupKeyCacheSize +
                ", maximumMetaDataCacheWeight=" + maximumMetaDataCacheWeight +
                ", initialMetaDataCacheSize=" + initialMetaDataCacheSize +
                ", lookupKeyCacheExecutorService=" + lookupKeyCacheExecutorService +
                ", lookupMetaDataCacheExecutorService=" + lookupMetaDataCacheExecutorService +
                ", lookupPageCacheExecutorService=" + lookupPageCacheExecutorService +
                ", maximumFileCacheSize=" + maximumFileCacheSize +
                ", intialFileCacheSize=" + intialFileCacheSize +
                ", fileCacheExecutorService=" + fileCacheExecutorService +
                ", flushDelaySeconds=" + flushDelaySeconds +
                ", dir=" + dir +
                ", metrics=" + metrics +
                '}';
    }
}
