package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class PagedFileMapper implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
                .recordStats()
                .<PageKey, FilePage>removalListener((key, value, cause) ->  {
                    log.debug("Called removal on {} with cause {}", key, cause);
                    if (value != null) value.flush();
                })
                .<PageKey, FilePage>build(key -> {
                    log.debug("new FilePage from {}", key);
                    final long pos = (long) pageSize * (long) key.getPage() ;
                    MappedByteBuffer buffer = fileCache
                            .getFileChannel(key.getFilePath())
                            .map(fileCache.readOnly() ? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE, pos, pageSize);
                    return new FilePage(key, pageSize, buffer);
                });
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
