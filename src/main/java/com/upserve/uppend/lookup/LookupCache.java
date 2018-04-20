package com.upserve.uppend.lookup;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.blobs.PageCache;
import com.upserve.uppend.metrics.MetricsStatsCounter;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.*;
import java.util.function.*;

public class LookupCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // An LRU cache of Lookup Keys
    private final Cache<PartitionLookupKey, Long> keyLongLookupCache;

    private final LoadingCache<LookupData, LookupMetadata> lookupMetaDataCache;

    // Pages loaded from LookupData files
    private final PageCache pageCache;

    public LookupCache(PageCache pagecache, int initialKeyCapacity, long maximumKeyWeight, ExecutorService executorServiceKeyCache, Supplier<StatsCounter> keyCacheMetricsSupplier, int intialMetaDataCapacity, long maximumMetaDataWeight, ExecutorService executorServiceMetaDataCache, Supplier<StatsCounter> metadataCacheMetricsSupplier) {
        this.pageCache = pagecache;

        Caffeine<PartitionLookupKey, Long> keyCacheBuilder = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .executor(executorServiceKeyCache)
                .initialCapacity(initialKeyCapacity)
                .maximumWeight(maximumKeyWeight)  // bytes
                .<PartitionLookupKey, Long>weigher((k ,v)-> k.weight());

        if (keyCacheMetricsSupplier != null) {
            keyCacheBuilder = keyCacheBuilder.recordStats(keyCacheMetricsSupplier);
        }

        keyLongLookupCache = keyCacheBuilder.<PartitionLookupKey, Long>build();


         Caffeine<LookupData, LookupMetadata> metadataCacheBuilder = Caffeine
                .<LookupData, LookupMetadata>newBuilder()
                .executor(executorServiceMetaDataCache)
                .initialCapacity(intialMetaDataCapacity)
                .maximumWeight(maximumMetaDataWeight)
                .<LookupData, LookupMetadata>weigher((k ,v) -> v.weight());

        if (metadataCacheMetricsSupplier != null) {
            metadataCacheBuilder = metadataCacheBuilder.recordStats(metadataCacheMetricsSupplier);
        }

        lookupMetaDataCache = metadataCacheBuilder
                .<LookupData, LookupMetadata>build(lookupData -> LookupMetadata.open(
                lookupData.getMetadataPath(),
                lookupData.getMetaDataGeneration()
        ));
    }

    public PageCache getPageCache(){
        return pageCache;
    }

    public void putLookup(PartitionLookupKey key, long val){
        keyLongLookupCache.put(key, val);
    }

    public Long getLong(PartitionLookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader){
        return keyLongLookupCache.get(lookupKey, cacheLoader);
    }

    public LookupMetadata getMetadata(LookupData key) {
        return lookupMetaDataCache.get(key);
    }

    public void putMetadata(LookupData key, LookupMetadata value) {
        lookupMetaDataCache.put(key, value);
    }

    public CacheStats pageStats() {
        return pageCache.stats();
    }

    public CacheStats keyStats() {
        return keyLongLookupCache.stats();
    }

    public CacheStats metadataStats() {
        return lookupMetaDataCache.stats();
    }

    @Override
    public void flush() {
        lookupMetaDataCache.invalidateAll();
        keyLongLookupCache.invalidateAll();
        pageCache.flush();
    }
}
