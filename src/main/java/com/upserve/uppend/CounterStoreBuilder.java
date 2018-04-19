package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.metrics.CounterStoreWithMetrics;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.concurrent.*;

public class CounterStoreBuilder {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Long lookup Cache Options
    private static final int DEFAULT_LOOKUP_HASH_SIZE = 256;
    private static final int DEFAULT_LOOKUP_PAGE_SIZE = 256 * 1024;
    private static final int DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE = 1024;
    private static final int DEFAULT_MAXIMUM_LOOKUP_PAGE_CACHE_SIZE = 16 * 1024;

    private static final long DEFAULT_MAXIMUM_LOOKUP_KEY_CACHE_WEIGHT = 1_000_000;
    private static final int DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE = 1000;

    private static final long DEFAULT_MAXIMUM_METADATA_CACHE_WEIGHT = 1_000_000;
    private static final int DEFAULT_INITIAL_METADATA_CACHE_SIZE = 1000;

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
    private static final int DEFAULT_MAXIMUM_FILE_CACHE_SIZE = 8_000;
    private static final int DEFAULT_INITIAL_FILE_CACHE_SIZE = 1000;

    private int maximumFileCacheSize = DEFAULT_MAXIMUM_FILE_CACHE_SIZE;
    private int intialFileCacheSize = DEFAULT_INITIAL_FILE_CACHE_SIZE;

    private ExecutorService fileCacheExecutorService = ForkJoinPool.commonPool();

    // Counter Store Options
    private static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    private int flushDelaySeconds = DEFAULT_FLUSH_DELAY_SECONDS;
    private Path dir;
    private MetricRegistry metrics;

    // Long lookup Cache Options
    public CounterStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.lookupHashSize = longLookupHashSize;
        return this;
    }

    public CounterStoreBuilder withLookupPageSize(int lookupPageSize) {
        this.lookupPageSize = lookupPageSize;
        return this;
    }

    public CounterStoreBuilder withInitialLookupPageCacheSize(int initialLookupPageCacheSize) {
        this.initialLookupPageCacheSize = initialLookupPageCacheSize;
        return this;
    }

    public CounterStoreBuilder withMaximumLookupPageCacheSize(int maximumLookupPageCacheSize) {
        this.maximumLookupPageCacheSize = maximumLookupPageCacheSize;
        return this;
    }

    public CounterStoreBuilder withMaximumLookupKeyCacheWeight(int maximumLookupKeyCacheWeight) {
        this.maximumLookupKeyCacheWeight = maximumLookupKeyCacheWeight;
        return this;
    }

    public CounterStoreBuilder withInitialLookupKeyCacheSize(int initialLookupKeyCacheSize) {
        this.initialLookupKeyCacheSize = initialLookupKeyCacheSize;
        return this;
    }

    public CounterStoreBuilder withMaximumMetaDataCacheWeight(int maximumMetaDataCacheWeight) {
        this.maximumMetaDataCacheWeight = maximumMetaDataCacheWeight;
        return this;
    }

    public CounterStoreBuilder withInitialMetaDataCacheSize(int initialMetaDataCacheSize) {
        this.initialMetaDataCacheSize = initialMetaDataCacheSize;
        return this;
    }

    public CounterStoreBuilder withLookupKeyCacheExecutorService(ExecutorService lookupKeyCacheExecutorService){
        this.lookupKeyCacheExecutorService = lookupKeyCacheExecutorService;
        return this;
    }

    public CounterStoreBuilder withLookupMetaDataCacheExecutorService(ExecutorService lookupMetaDataCacheExecutorService){
        this.lookupMetaDataCacheExecutorService = lookupMetaDataCacheExecutorService;
        return this;
    }

    public CounterStoreBuilder withLookupPageCacheExecutorService(ExecutorService lookupPageCacheExecutorService){
        this.lookupPageCacheExecutorService = lookupPageCacheExecutorService;
        return this;
    }

    // File Cache Options
    public CounterStoreBuilder withMaximumFileCacheSize(int maximumFileCacheSize) {
        this.maximumFileCacheSize = maximumFileCacheSize;
        return this;
    }

    public CounterStoreBuilder withIntialFileCacheSize(int intialFileCacheSize) {
        this.intialFileCacheSize = intialFileCacheSize;
        return this;
    }

    public CounterStoreBuilder withFileCacheExecutorService(ExecutorService fileCacheExecutorService){
        this.fileCacheExecutorService = fileCacheExecutorService;
        return this;
    }

    // Counter Store Options
    public CounterStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    public CounterStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public CounterStoreBuilder withMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
        return this;
    }

    public CounterStore build() {
        return build(false);
    }

    public CounterStore build(boolean readOnly) {
        if (readOnly && flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS) throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        CounterStore store = new FileCounterStore(readOnly, this);
        if (metrics != null) {
            store = new CounterStoreWithMetrics(store, metrics);
        }
        return store;
    }

    public ReadOnlyCounterStore buildReadOnly() {
        if (flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS) throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        return new FileCounterStore(true, this);
    }

    private static CounterStoreBuilder defaultTestBuilder = new CounterStoreBuilder()
            .withInitialLookupKeyCacheSize(64)
            .withMaximumLookupKeyCacheWeight(100 * 1024)
            .withInitialMetaDataCacheSize(64)
            .withMaximumMetaDataCacheWeight(100 * 1024)
            .withIntialFileCacheSize(128)
            .withMaximumFileCacheSize(1024)
            .withLongLookupHashSize(16)
            .withLookupPageSize(16*1024);

    public static CounterStoreBuilder getDefaultTestBuilder(){
        return defaultTestBuilder;
    }

    public int getLookupHashSize() {
        return lookupHashSize;
    }

    public int getLookupPageSize() {
        return lookupPageSize;
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
}
