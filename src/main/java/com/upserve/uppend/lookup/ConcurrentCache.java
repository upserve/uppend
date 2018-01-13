package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.AutoFlusher;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.function.*;

public class ConcurrentCache {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ConcurrentHashMap<Path, CacheEntry> cache;
    private final int cacheSize;

    private final AtomicLong expired = new AtomicLong();
    private final AtomicBoolean sizeExceeded = new AtomicBoolean();

    public ConcurrentCache(int cacheSize, float loadFactor){
        this.cache = new ConcurrentHashMap<>(cacheSize, loadFactor);
        this.cacheSize = cacheSize;
    }

    private static class CacheEntry{
        protected final AtomicLong lastTouched;
        protected final AtomicBoolean tombStone;
        protected final AtomicReference<LookupData> lookupData;

        protected CacheEntry(AtomicLong lastTouched, AtomicBoolean tombStone, LookupData lookupData){
            this.lastTouched = lastTouched;
            this.lookupData = new AtomicReference<>(lookupData);
            this.tombStone = tombStone;
        }

        protected CacheEntry(LookupData lookupData){
            this(new AtomicLong(System.nanoTime()), new AtomicBoolean(false), lookupData);
        }

    }

    public <T> T compute(Path path, Function<LookupData, T> function){
        AtomicReference<T> result = new AtomicReference<>(null);
        cache.compute(path, (Path keyPath, CacheEntry cacheEntry) -> {
            if (cacheEntry == null){
                log.trace("cache loading {}", keyPath);
                LookupData lookupData = new LookupData(keyPath.resolve("data"), keyPath.resolve("meta"));
                result.set(function.apply(lookupData));
                return new CacheEntry(lookupData);
            } else {
                cacheEntry.lastTouched.set(System.nanoTime());
                result.set(function.apply(cacheEntry.lookupData.get()));
                return cacheEntry;
            }
        });

        sizeExceeded.set(cache.size() > cacheSize);
        return result.get();
    }

    public <T> T evaluateIfPresent(Path path, Function<LookupData, T> function){
        AtomicReference<T> result = new AtomicReference<>(null);
        cache.computeIfPresent(path, (keyPath, cacheEntry) ->
        {
            result.set(function.apply(cacheEntry.lookupData.get()));
            return cacheEntry;
        });
        return result.get();
    }

    public void forEach(BiConsumer<Path, LookupData> biConsumer){
        cache.forEach((path, cacheEntry) -> {
               biConsumer.accept(path, cacheEntry.lookupData.get());
        });
    }

    public int size(){
        return cache.size();
    }

    /**
     * submit job to reap an expired cache exactly once
     */
    public void reapExpired(){
        if (sizeExceeded.get()){
            cache
                    .entrySet()
                    .stream()
                    .sorted(Comparator.comparing(entry -> entry.getValue().lastTouched.get()))
                    .skip(cacheSize)
                    .forEach(cacheEntry -> {
                        if (cacheEntry.getValue().tombStone.compareAndSet(false, true)){
                            AutoFlusher.flushExecPool.submit(expire(cacheEntry.getKey()));
                        }
                    });
        }

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            log.error("Reaper sleep interrupted");
        }

        AutoFlusher.flushExecPool.submit(() -> reapExpired());
    }

    public Runnable expire(Path path){
        return () -> {
            cache.compute(path, (keyPath, cacheEntry)->{

                try {
                    cacheEntry.lookupData.get().close();
                } catch (IOException e) {
                    log.error("Could not close LookupData for path {}", path);
                } catch (Exception e){
                    log.error("Unexpected exception while closing {}", path);
                }

                return null;
            });
        };
    }

}
