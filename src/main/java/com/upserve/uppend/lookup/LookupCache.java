package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.upserve.uppend.blobs.PageCache;
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

    public LookupCache(PageCache pagecache, int initialKeyCapacity, long maximumKeyWeight, int intialMetaDataCapacity, long maximumMetaDataWeight) {
        this(pagecache, initialKeyCapacity, maximumKeyWeight, intialMetaDataCapacity, maximumMetaDataWeight, ForkJoinPool.commonPool(), ForkJoinPool.commonPool());
    }

    public LookupCache(PageCache pagecache, int initialKeyCapacity, long maximumKeyWeight, int intialMetaDataCapacity, long maximumMetaDataWeight, ExecutorService executorServiceKeyCache, ExecutorService executorServiceMetaDataCache) {
        this.pageCache = pagecache;

        keyLongLookupCache = Caffeine
                .<PartitionLookupKey, Long>newBuilder()
                .executor(executorServiceKeyCache)
                .initialCapacity(initialKeyCapacity)
                .maximumWeight(maximumKeyWeight)  // bytes
                .<PartitionLookupKey, Long>weigher((k ,v)-> k.weight())
                .<PartitionLookupKey, Long>build();


        lookupMetaDataCache = Caffeine
                .<LookupData, LookupMetadata>newBuilder()
                .executor(executorServiceMetaDataCache)
                .initialCapacity(intialMetaDataCapacity)
                .maximumWeight(maximumMetaDataWeight)
                .<LookupData, LookupMetadata>weigher((k ,v) -> v.weight())
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


    @Override
    public void flush() {
        lookupMetaDataCache.invalidateAll();
        keyLongLookupCache.invalidateAll();
        pageCache.flush();
    }
}
