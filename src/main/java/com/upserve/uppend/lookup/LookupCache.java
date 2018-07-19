package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.*;
import org.slf4j.Logger;

import java.io.Flushable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;

public class LookupCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // An LRU cache of Lookup Keys
    private final Cache<PartitionLookupKey, Long> keyLongLookupCache;

    private final boolean keyCacheActive;

    private final LongAdder keysFlushed;
    private final LongAdder lookupsFlushed;

    public LookupCache(int initialKeyCapacity, long maximumKeyWeight, ExecutorService executorServiceKeyCache, Supplier<StatsCounter> keyCacheMetricsSupplier) {

        Caffeine<PartitionLookupKey, Long> keyCacheBuilder = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .executor(executorServiceKeyCache)
                .initialCapacity(initialKeyCapacity)
                .maximumWeight(maximumKeyWeight)  // bytes
                .<PartitionLookupKey, Long>weigher((k, v) -> k.weight());

        if (keyCacheMetricsSupplier != null) {
            keyCacheBuilder = keyCacheBuilder.recordStats(keyCacheMetricsSupplier);
        }

        keyCacheActive = maximumKeyWeight > 0;

        keyLongLookupCache = keyCacheBuilder.<PartitionLookupKey, Long>build();

        keysFlushed = new LongAdder();
        lookupsFlushed = new LongAdder();
    }

    public FlushStats getFlushStats() {
        return new FlushStats(keysFlushed.longValue(), lookupsFlushed.longValue());
    }

    public void addFlushCount(long val){
        keysFlushed.add(val);
        lookupsFlushed.increment();
    }

    public boolean isKeyCacheActive() {
        return keyCacheActive;
    }

    public void putLookup(PartitionLookupKey key, long val) {
        keyLongLookupCache.put(key, val);
    }

    public Long getLong(PartitionLookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader) {
        return keyLongLookupCache.get(lookupKey, cacheLoader);
    }

    public CacheStats keyStats() {
        if (keyCacheActive) {
            return keyLongLookupCache.stats();
        } else {
            return CacheStats.empty();
        }
    }

    @Override
    public void flush() {
        keyLongLookupCache.invalidateAll();
    }
}
