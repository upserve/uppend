package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.LookupCache;
import com.upserve.uppend.metrics.MetricsStatsCounter;

import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.function.*;

public class FileStoreBuilder <T extends FileStoreBuilder<T>> {

    static final String FILE_CACHE_METRICS = "FileCache";
    static final String LOOKUP_PAGE_CACHE_METRICS = "LookupPageCache";
    static final String BLOB_PAGE_CACHE_METRICS = "BlobPageCache";
    static final String LOOKUP_KEY_CACHE_METRICS = "LookupKeyCache";
    static final String METADATA_CACHE_METRICS = "MetadataCache";

    // Long lookup Cache Options
    public static final int DEFAULT_LOOKUP_HASH_SIZE = 256;
    public static final int DEFAULT_LOOKUP_PAGE_SIZE = 1024 * 1024;
    public static final int DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE = 1024;
    public static final int DEFAULT_MAXIMUM_LOOKUP_PAGE_CACHE_SIZE = 16 * 1024;

    public static final long DEFAULT_MAXIMUM_LOOKUP_KEY_CACHE_WEIGHT = 1_000_000;
    public static final int DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE = 1000;

    public static final long DEFAULT_MAXIMUM_METADATA_CACHE_WEIGHT = 1_000_000;
    public static final int DEFAULT_INITIAL_METADATA_CACHE_SIZE = 1000;

    int lookupHashSize = DEFAULT_LOOKUP_HASH_SIZE;

    int lookupPageSize = DEFAULT_LOOKUP_PAGE_SIZE;
    int initialLookupPageCacheSize = DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE;
    int maximumLookupPageCacheSize = DEFAULT_MAXIMUM_LOOKUP_PAGE_CACHE_SIZE;

    long maximumLookupKeyCacheWeight = DEFAULT_MAXIMUM_LOOKUP_KEY_CACHE_WEIGHT;
    int initialLookupKeyCacheSize = DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE;

    long maximumMetaDataCacheWeight = DEFAULT_MAXIMUM_METADATA_CACHE_WEIGHT;
    int initialMetaDataCacheSize = DEFAULT_INITIAL_METADATA_CACHE_SIZE;

    ExecutorService lookupKeyCacheExecutorService = ForkJoinPool.commonPool();
    ExecutorService lookupMetaDataCacheExecutorService = ForkJoinPool.commonPool();
    ExecutorService lookupPageCacheExecutorService = ForkJoinPool.commonPool();

    // File Cache Options
    public static final int DEFAULT_MAXIMUM_FILE_CACHE_SIZE = 8_000;
    public static final int DEFAULT_INITIAL_FILE_CACHE_SIZE = 1000;

    int maximumFileCacheSize = DEFAULT_MAXIMUM_FILE_CACHE_SIZE;
    int intialFileCacheSize = DEFAULT_INITIAL_FILE_CACHE_SIZE;

    ExecutorService fileCacheExecutorService = ForkJoinPool.commonPool();

    // Store Options
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    int flushDelaySeconds = DEFAULT_FLUSH_DELAY_SECONDS;
    Path dir = null;
    MetricRegistry storeMetricsRegistry = null;
    boolean storeMetrics = false;
    MetricRegistry cacheMetricsRegistry = null;
    boolean cacheMetrics = false;



    // Long lookup Cache Options
    @SuppressWarnings("unchecked")
    public T withLongLookupHashSize(int longLookupHashSize) {
        this.lookupHashSize = longLookupHashSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupPageSize(int lookupPageSize) {
        this.lookupPageSize = lookupPageSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withInitialLookupPageCacheSize(int initialLookupPageCacheSize) {
        this.initialLookupPageCacheSize = initialLookupPageCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMaximumLookupPageCacheSize(int maximumLookupPageCacheSize) {
        this.maximumLookupPageCacheSize = maximumLookupPageCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMaximumLookupKeyCacheWeight(int maximumLookupKeyCacheWeight) {
        this.maximumLookupKeyCacheWeight = maximumLookupKeyCacheWeight;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withInitialLookupKeyCacheSize(int initialLookupKeyCacheSize) {
        this.initialLookupKeyCacheSize = initialLookupKeyCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMaximumMetaDataCacheWeight(int maximumMetaDataCacheWeight) {
        this.maximumMetaDataCacheWeight = maximumMetaDataCacheWeight;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withInitialMetaDataCacheSize(int initialMetaDataCacheSize) {
        this.initialMetaDataCacheSize = initialMetaDataCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupKeyCacheExecutorService(ExecutorService lookupKeyCacheExecutorService){
        this.lookupKeyCacheExecutorService = lookupKeyCacheExecutorService;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupMetaDataCacheExecutorService(ExecutorService lookupMetaDataCacheExecutorService){
        this.lookupMetaDataCacheExecutorService = lookupMetaDataCacheExecutorService;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupPageCacheExecutorService(ExecutorService lookupPageCacheExecutorService){
        this.lookupPageCacheExecutorService = lookupPageCacheExecutorService;
        return (T) this;
    }

    // File Cache Options
    @SuppressWarnings("unchecked")
    public T withMaximumFileCacheSize(int maximumFileCacheSize) {
        this.maximumFileCacheSize = maximumFileCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withIntialFileCacheSize(int intialFileCacheSize) {
        this.intialFileCacheSize = intialFileCacheSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withFileCacheExecutorService(ExecutorService fileCacheExecutorService){
        this.fileCacheExecutorService = fileCacheExecutorService;
        return (T) this;
    }

    // Append Store Options
    @SuppressWarnings("unchecked")
    public T withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withDir(Path dir) {
        this.dir = dir;
        return (T) this;
    }

    /**
     * The builder will wrap the built store in a class that computes storeMetrics for each operation
     * @param metrics a CodaHale storeMetrics registry
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withStoreMetrics(MetricRegistry metrics) {
        this.storeMetricsRegistry = metrics;
        this.storeMetrics = true;
        return (T) this;
    }

    /**
     * Apply a MetricsCounter to the Caffeine Caches used by the store
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withCacheMetrics() {
        this.cacheMetrics = true;
        return (T) this;
    }

    /**
     * Apply a MetricsCounter to the Caffeine Caches used by the store using a CodaHale MetricsRegistry as the counter
     * @param metrics a CodaHale storeMetrics registry
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withCacheMetrics(MetricRegistry metrics) {
        this.cacheMetricsRegistry = metrics;
        this.cacheMetrics = true;
        return (T) this;
    }

    /**
     * Return a StatsCounterSupplier for use in the Caffeine builder or null if cacheMetrics is false
     * @param elements the string elements to use in registering metrics for this cache
     * @return the Supplier or null
     */
    public Supplier<StatsCounter> metricsSupplier(String...elements) {
        if (!cacheMetrics) return null;
        if (cacheMetricsRegistry != null) {
            return () -> new MetricsStatsCounter(cacheMetricsRegistry, String.join(".", elements));
        } else {
            return ConcurrentStatsCounter::new;
        }
    }

    public FileCache buildFileCache(boolean readOnly, String metricsPrefix){
        return new FileCache(
                getIntialFileCacheSize(),
                getMaximumFileCacheSize(),
                getFileCacheExecutorService(),
                metricsSupplier(metricsPrefix, FILE_CACHE_METRICS),
                readOnly);
    }

    public PageCache buildLookupPageCache(FileCache fileCache, String metricsPrefix) {
        return new PageCache(
                fileCache,
                getLookupPageSize(),
                getInitialLookupPageCacheSize(),
                getMaximumLookupPageCacheSize(),
                getLookupPageCacheExecutorService(),
                metricsSupplier(metricsPrefix, LOOKUP_PAGE_CACHE_METRICS)
        );
    }

    public LookupCache buildLookupCache(PageCache pageCache, String metricsPrefix){
        return new LookupCache(
                pageCache,
                getInitialLookupKeyCacheSize(),
                getMaximumLookupKeyCacheWeight(),
                getLookupKeyCacheExecutorService(),
                metricsSupplier(metricsPrefix, LOOKUP_KEY_CACHE_METRICS),
                getInitialMetaDataCacheSize(),
                getMaximumMetaDataCacheWeight(),
                getLookupMetaDataCacheExecutorService(),
                metricsSupplier(metricsPrefix, METADATA_CACHE_METRICS)
        );
    }


    public int getLookupHashSize() {
        return lookupHashSize;
    }

    public int getLookupPageSize() {
        return lookupPageSize;
    }

    public boolean isStoreMetrics() {
        return storeMetrics;
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

    public int getFlushDelaySeconds() {
        return flushDelaySeconds;
    }

    public Path getDir() {
        return dir;
    }

    public MetricRegistry getStoreMetricsRegistry() {
        return storeMetricsRegistry;
    }

    public MetricRegistry getCacheMetricsRegistry() {
        return cacheMetricsRegistry;
    }

    public boolean isCacheMetrics() {
        return cacheMetrics;
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
}
