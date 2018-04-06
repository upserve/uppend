package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class PagedFileMapper implements Flushable {

    private final int pageSize;
    private final FileCache fileCache;
    private final LoadingCache<PageKey, FilePage> pageCache;

    public PagedFileMapper(int pageSize, int initialCacheSize, int maximumCacheSize, FileCache fileCache) {
        this.pageSize = pageSize;
        this.fileCache = fileCache;
        this.pageCache = Caffeine
                .<PageKey, FilePage>newBuilder()
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .<PageKey, FilePage>build(key -> new FilePage(key, pageSize, fileCache));
    }

    FilePage getPage(Path filePath, long pos){
        return getPage(new PageKey(filePath, page(pos)));
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
    public boolean readOnly() { return fileCache.readOnly(); }

    public CacheStats cacheStats(){
        return pageCache.stats();
    }

    public FileCache getFileCache() {
        return fileCache;
    }

    int getPageSize() {
        return pageSize;
    }

    @Override
    public void flush() {
        pageCache.invalidateAll();
    }
}
