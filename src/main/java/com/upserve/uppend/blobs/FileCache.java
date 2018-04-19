package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;

/**
 * A cache of file handles.
 *
 * TODO Removal is async and when the cache is under heavy load, files get closed while still in use
 */
public class FileCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LoadingCache<Path, FileChannel> fileCache;

    private final boolean readOnly;

    public FileCache(int initialCacheSize, int maximumCacheSize, boolean readOnly) {
        this(initialCacheSize, maximumCacheSize, readOnly, ForkJoinPool.commonPool());
    }

    public FileCache(int initialCacheSize, int maximumCacheSize, boolean readOnly, ExecutorService executorService){

        this.readOnly = readOnly;

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ};
        } else {
            openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }

        this.fileCache = Caffeine
                .<Path, FileChannel>newBuilder()
                .executor(executorService)
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .expireAfterAccess(300, TimeUnit.DAYS)
                .recordStats()
                .<Path, FileChannel>removalListener((key, value, cause) ->  {
                    log.debug("Called removal on {} with cause {}", key, cause);
                    if (value != null && value.isOpen()) {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("Unable to close file {}", key, e);
                        }
                    }
                })
                .<Path, FileChannel>build(path -> {
                    log.debug("opening {} in file cache", path);
                    return FileChannel.open(path, openOptions);
                });
    }

    public boolean readOnly(){
        return readOnly;
    }

    public FileChannel getFileChannel(Path path){
        return fileCache.get(path);
    }

    public FileChannel getFileChannelIfPresent(Path path) { return fileCache.getIfPresent(path); }

    @Override
    public void flush(){
        fileCache.invalidateAll();
    }

    public CacheStats stats(){
        return fileCache.stats();
    }
}
