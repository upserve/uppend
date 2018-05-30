package com.upserve.uppend.lookup;

import java.util.function.Function;

/**
 * AppendStorePartition level view of the store lookup cache
 */
public class PartitionLookupCache {

    private final LookupCache lookupCache;
    private final String partition;


    public static PartitionLookupCache create(String partition, LookupCache lookupCache) {
        return new PartitionLookupCache(partition.intern(), lookupCache);
    }

    public PartitionLookupCache(String partition, LookupCache lookupCache) {
        this.partition = partition;
        this.lookupCache = lookupCache;
    }

    public String getPartition() {
        return partition;
    }

    public void putLookup(LookupKey key, long val) {
        lookupCache.putLookup(new PartitionLookupKey(partition, key), val);
    }

    public Long getLong(LookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader) {
        return lookupCache.getLong(new PartitionLookupKey(partition, lookupKey), cacheLoader);
    }

    public boolean isKeyCacheActive(){
        return lookupCache.isKeyCacheActive();
    }

    public LookupMetadata getMetadata(LookupData key) {
        return lookupCache.getMetadata(key);
    }

    public void putMetadata(LookupData key, LookupMetadata value) {
        lookupCache.putMetadata(key, value);
    }

    public void addFlushCount(long val){
        lookupCache.addFlushCount(val);
    }
}
