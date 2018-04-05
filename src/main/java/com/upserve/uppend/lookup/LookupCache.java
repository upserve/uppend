package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.PagedFileMapper;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public class LookupCache {

    // An LRU cache of Lookup Keys
    private final Cache<PartitionLookupKey, Long> lruReadCache;

    // Pages loaded from LookupData files
    private final PagedFileMapper pageCache;

    public LookupCache(PagedFileMapper pagecache) {
        this.pageCache = pagecache;

        lruReadCache = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .maximumWeight(1_000_000)  // bytes
                .<PartitionLookupKey, Long>weigher((k ,v)-> k.weight())
                .<PartitionLookupKey, Long>build();

    }


    public PagedFileMapper getPageCache(){
        return pageCache;
    }

    public Long getLong(PartitionLookupKey lookupKey, Function<PartitionLookupKey, Long> cacheLoader){
        return lruReadCache.get(lookupKey, cacheLoader);
    }

}
