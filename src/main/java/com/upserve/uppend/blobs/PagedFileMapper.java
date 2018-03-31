package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class PagedFileMapper {

    private final int pageSize;

    private final LoadingCache<PageKey, FilePage> pageCache;

    public PagedFileMapper(int pageSize) {
        this.pageSize = pageSize;

        this.pageCache = Caffeine
                .<PageKey, FilePage>newBuilder()
                .initialCapacity(1000)
                .<PageKey, FilePage>build(key -> new FilePage(key, pageSize));
    }

    FilePage getPage(FileChannel chan, Path file, long pos){
        return getPage(new PageKey(file, chan, page(pos)));
    }

    private FilePage getPage(PageKey key){
        return pageCache.get(key);
    }

    private int page(long pos) {
        long pageIndexLong = pos / pageSize;
        if (pageIndexLong > Integer.MAX_VALUE) {
            throw new RuntimeException("page index exceeded max int: " + pageIndexLong);
        }
        return (int) pageIndexLong;
    }

    public CacheStats cacheStats(){
        return pageCache.stats();
    }

    int getPageSize() {
        return pageSize;
    }
}
