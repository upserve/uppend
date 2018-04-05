package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.PagedFileMapper;

import java.util.OptionalLong;
import java.util.function.*;

/**
 * Partition level view of the store lookup cache
 */
public class PartitionLookupCache {

    private final LookupCache lookupCache;
    private final String partition;


    public static PartitionLookupCache create(String partition, LookupCache lookupCache){
        return new PartitionLookupCache(partition.intern(), lookupCache);
    }

    public PartitionLookupCache(String partition, LookupCache lookupCache) {
        this.partition = partition;
        this.lookupCache = lookupCache;
    }

    public PagedFileMapper getPageCache(){
        return lookupCache.getPageCache();
    }

    public Long getLong(LookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader){
        return lookupCache.getLong(new PartitionLookupKey(partition, lookupKey), cacheLoader);
    }
}
