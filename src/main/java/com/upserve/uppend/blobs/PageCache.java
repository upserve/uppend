package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.*;
import org.slf4j.Logger;

import java.io.Flushable;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.*;

/**
 * A cache of memory mapped file pages
 * <p>
 * Concurrent writes to random new pages cause the JVM to crash in JDK < 9
 * Attempted solution to force the buffer when it extends the file failed to fix the issue. See testHammerPageCache
 * Uppend should not make concurrent writes to multiple pages in normal operation - unless blobs are larger than a page
 */
public class PageCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Cache<PageKey, Page> pageCache;
    private final int pageSize;

    public PageCache(int pageSize, int initialCacheSize, int maximumCacheSize, ExecutorService executorService, Supplier<StatsCounter> metricsSupplier) {
        this.pageSize = pageSize;

        Caffeine<PageKey, Page> cacheBuilder = Caffeine
                .<PageKey, Page>newBuilder()
                .executor(executorService)
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .<PageKey, Page>removalListener((key, value, cause) -> {
                    log.debug("Called removal on {} with cause {}", key, cause);
                });

        if (metricsSupplier != null) {
            cacheBuilder = cacheBuilder.recordStats(metricsSupplier);
        }

        this.pageCache = cacheBuilder
                .<PageKey, Page>build();

    }

    public int getPageSize() {
        return pageSize;
    }

    Page get(long pos, Path path, Function<PageKey, Page> pageLoader) {
        return pageCache.get(new PageKey(path, pos), pageLoader);
    }

    Optional<Page> getIfPresent(VirtualPageFile virtualPageFile, long pos) {
        return Optional.ofNullable(pageCache.getIfPresent(new PageKey(virtualPageFile.getFilePath(), pos)));
    }

    public CacheStats stats() {
        return pageCache.stats();
    }

    @Override
    public void flush() {
        pageCache.invalidateAll();
    }
}
