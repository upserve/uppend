package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;

/**
 * A cache of open file handles.
 * If it is desirable to explicitly manage file close - we can add a method to invalidate a path and make all
 * the objects using Files closable but it seems better to just close the cache when everything is done.
 */
public class FileCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LoadingCache<Path, FileChannel> fileCache;

    private final boolean readOnly;

    public FileCache(int initialCacheSize, int maximumCacheSize, boolean readOnly){

        this.readOnly = readOnly;

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ};
        } else {
            openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }

        this.fileCache = Caffeine
                .<Path, FileChannel>newBuilder()
//                .executor(Executors.newCachedThreadPool())
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .expireAfterAccess(300, TimeUnit.DAYS)
                .<Path, FileChannel>removalListener((key, value, cause) ->  {
                    log.warn("Called removal on {} with {}", key, cause);
                    if (value != null && value.isOpen()) {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("Unable to close file {}", key, e);
                        }
                    }
                })
                .<Path, FileChannel>build(path -> {
                    log.warn("opening {}", path);
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
}
