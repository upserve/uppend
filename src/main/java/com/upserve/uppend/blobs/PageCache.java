package com.upserve.uppend.blobs;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.metrics.MetricsStatsCounter;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * A cache of memory mapped file pages
 * <p>
 * TODO Concurrent writes to random new pages cause the JVM to crash
 * Attempted solution to force the buffer when it extends the file failed to fix the issue. See testHammerPageCache
 * Uppend should not make concurrent writes to multiple pages in normal operation - unless blobs are larger than a page
 */
public class PageCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int pageSize;
    private final FileCache fileCache;
    private final LoadingCache<PageKey, FilePage> pageCache;

    public PageCache(FileCache fileCache, int pageSize, int initialCacheSize, int maximumCacheSize, ExecutorService executorService, Supplier<StatsCounter> metricsSupplier) {
        this.pageSize = pageSize;
        this.fileCache = fileCache;
        Caffeine<PageKey, FilePage> cacheBuilder = Caffeine
                .<PageKey, FilePage>newBuilder()
                .executor(executorService)
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .<PageKey, FilePage>removalListener((key, value, cause) -> {
                    log.debug("Called removal on {} with cause {}", key, cause);
//                    if (value != null) value.flush();
                });

        if (metricsSupplier != null) {
            cacheBuilder = cacheBuilder.recordStats(metricsSupplier);
        }

        FileChannel.MapMode mode = fileCache.readOnly() ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;

        this.pageCache = cacheBuilder
                .<PageKey, FilePage>build(key -> {
                    log.debug("new FilePage from {}", key);
                    final long filePosition = pageSize * key.getPage();
                    MappedByteBuffer buffer = fileCache.getFileChannel(key.getFilePath()).map(mode, filePosition, pageSize);
                    return new FilePage(buffer);
                });

    }

    FilePage getPage(Path filePath, long pos) {
        long pageIndexLong = pos / pageSize;
        return getPage(new PageKey(filePath, pageIndexLong)
        );
    }

    private FilePage getPage(PageKey key) {
        return pageCache.get(key);
    }

    public int pagePosition(long pos) {
        return (int) (pos % (long) pageSize);
    }

    public boolean readOnly() {
        return fileCache.readOnly();
    }

    public CacheStats stats() {
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
