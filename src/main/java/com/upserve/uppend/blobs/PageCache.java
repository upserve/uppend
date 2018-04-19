package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.sun.javaws.exceptions.InvalidArgumentException;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;

public class PageCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int pageSize;
    private final FileCache fileCache;
    private final LoadingCache<PageKey, FilePage> pageCache;

    public PageCache(int pageSize, int initialCacheSize, int maximumCacheSize, FileCache fileCache) {
        this.pageSize = pageSize;
        this.fileCache = fileCache;
        this.pageCache = Caffeine
                .<PageKey, FilePage>newBuilder()
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .executor(new ForkJoinPool())
                .recordStats()
                .<PageKey, FilePage>removalListener((key, value, cause) ->  {
                    log.debug("Called removal on {} with cause {}", key, cause);
                    if (value != null) value.flush();
                })
                .<PageKey, FilePage>build(key -> {
                    log.debug("new FilePage from {}", key);
                    final long filePosition = pageSize * key.getPage() ;
                    MappedByteBuffer buffer = fileCache
                            .getFileChannel(key.getFilePath())
                            .map(fileCache.readOnly() ? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE, filePosition, pageSize);
                    return new FilePage(buffer);
                });
    }

    FilePage getPage(Path filePath, long pos){
        long pageIndexLong = pos / pageSize;
        return getPage(new PageKey(filePath, pageIndexLong)
        );
    }

    private FilePage getPage(PageKey key){
        return pageCache.get(key);
    }

    public int pagePosition(long pos){
        return (int) (pos % (long) pageSize);
    }

    public boolean readOnly() { return fileCache.readOnly(); }

    public CacheStats stats(){
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
