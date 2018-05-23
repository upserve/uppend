package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.*;
import org.slf4j.Logger;

import java.io.Flushable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.*;
import java.util.function.*;

public class LookupCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // An LRU cache of Lookup Keys
    private final Cache<PartitionLookupKey, Long> keyLongLookupCache;

    private final LoadingCache<LookupData, LookupMetadata> lookupMetaDataCache;


    public LookupCache(int initialKeyCapacity, long maximumKeyWeight, ExecutorService executorServiceKeyCache, Supplier<StatsCounter> keyCacheMetricsSupplier, int intialMetaDataCapacity, long maximumMetaDataWeight, int metadataTTL, ExecutorService executorServiceMetaDataCache, Supplier<StatsCounter> metadataCacheMetricsSupplier) {

        Caffeine<PartitionLookupKey, Long> keyCacheBuilder = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .executor(executorServiceKeyCache)
                .initialCapacity(initialKeyCapacity)
                .maximumWeight(maximumKeyWeight)  // bytes
                .<PartitionLookupKey, Long>weigher((k, v) -> k.weight());

        if (keyCacheMetricsSupplier != null) {
            keyCacheBuilder = keyCacheBuilder.recordStats(keyCacheMetricsSupplier);
        }

        keyLongLookupCache = keyCacheBuilder.<PartitionLookupKey, Long>build();


        Caffeine<LookupData, LookupMetadata> metadataCacheBuilder = Caffeine
                .<LookupData, LookupMetadata>newBuilder()
                .executor(executorServiceMetaDataCache)
                .initialCapacity(intialMetaDataCapacity)
                .maximumWeight(maximumMetaDataWeight)
                .<LookupData, LookupMetadata>weigher((k, v) -> v.weight());

        if (metadataTTL > 0) {
            metadataCacheBuilder.expireAfterWrite(metadataTTL, TimeUnit.SECONDS);
        }

        if (metadataCacheMetricsSupplier != null) {
            metadataCacheBuilder = metadataCacheBuilder.recordStats(metadataCacheMetricsSupplier);
        }

        lookupMetaDataCache = metadataCacheBuilder
                .<LookupData, LookupMetadata>build(LookupData::loadMetadata);

    }

    public void putLookup(PartitionLookupKey key, long val) {
        keyLongLookupCache.put(key, val);
    }

    public Long getLong(PartitionLookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader) {
        return keyLongLookupCache.get(lookupKey, cacheLoader);
    }

    public LookupMetadata getMetadata(LookupData key) {
        return lookupMetaDataCache.get(key);
    }

    public void putMetadata(LookupData key, LookupMetadata value) {
        lookupMetaDataCache.put(key, value);
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
    }
}
