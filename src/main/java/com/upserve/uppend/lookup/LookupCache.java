package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.upserve.uppend.blobs.PagedFileMapper;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

public class LookupCache implements Flushable {

    // An LRU cache of Lookup Keys
    private final Cache<PartitionLookupKey, Long> keyLongLookupCache;

    private final LoadingCache<LookupData, LookupMetadata> lookupMetaDataCache;

    // Pages loaded from LookupData files
    private final PagedFileMapper pageCache;

    public LookupCache(PagedFileMapper pagecache) {
        this.pageCache = pagecache;

        keyLongLookupCache = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .maximumWeight(1_000_000)  // bytes
                .<PartitionLookupKey, Long>weigher((k ,v)-> k.weight())
                .<PartitionLookupKey, Long>build();


        lookupMetaDataCache = Caffeine
                .<LookupData, LookupMetadata>newBuilder()
                .maximumWeight(1_000_000)
                .<LookupData, LookupMetadata>weigher((k ,v) -> v.weight())
                .<LookupData, LookupMetadata>build(lookupData -> new LookupMetadata(
                        lookupData.getMetadataPath(),
                        lookupData.getMetaDataGeneration()
                ));
    }

    public PagedFileMapper getPageCache(){
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


    @Override
    public void flush() {
        lookupMetaDataCache.invalidateAll();
        keyLongLookupCache.invalidateAll();
        pageCache.flush();
    }
}
