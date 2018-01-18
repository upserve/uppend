package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.AutoFlusher;
import com.upserve.uppend.util.Futures;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public class ConcurrentCache {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ConcurrentHashMap<Path, CacheEntry> cache;
    private final int cacheSize;

    public ConcurrentCache(int cacheSize, float loadFactor) {
        this.cache = new ConcurrentHashMap<>(cacheSize, loadFactor);
        this.cacheSize = cacheSize;

        AutoFlusher.flushExecPool.submit(this::reapExpired);
    }

    public void clear() {
        this.cache.clear();
    }

    public void flush() {
        ArrayList<Future> futures = new ArrayList<>();
        cache.forEach((path, entry) ->
                futures.add(AutoFlusher.flushExecPool.submit(() -> {
                            try {
                                log.trace("cache flushing {}", path);
                                entry.lookupData.get().flush();
                                log.trace("cache flushed {}", path);
                            } catch (Exception e) {
                                log.error("unable to flush " + path, e);
                            }
                        }
                )));
        Futures.getAll(futures);

    }

    private static class CacheEntry {
        protected final AtomicLong lastTouched;
        protected final AtomicBoolean tombStone;
        protected final AtomicReference<LookupData> lookupData;

        protected CacheEntry(AtomicLong lastTouched, AtomicBoolean tombStone, LookupData lookupData) {
            this.lastTouched = lastTouched;
            this.lookupData = new AtomicReference<>(lookupData);
            this.tombStone = tombStone;
        }

        protected CacheEntry(LookupData lookupData) {
            this(new AtomicLong(System.nanoTime()), new AtomicBoolean(false), lookupData);
        }

    }

    public <T> T compute(Path path, Function<LookupData, T> function) {
        AtomicReference<T> result = new AtomicReference<>(null);
        cache.compute(path, (Path keyPath, CacheEntry cacheEntry) -> {
            if (cacheEntry == null) {
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
        return result.get();
    }

    public <T> T evaluateIfPresent(Path path, Function<LookupData, T> function) {
        AtomicReference<T> result = new AtomicReference<>(null);
        cache.computeIfPresent(path, (keyPath, cacheEntry) ->
        {
            result.set(function.apply(cacheEntry.lookupData.get()));
            return cacheEntry;
        });
        return result.get();
    }

    public void purge() {
        expireStream(cache.entrySet().stream());
    }


    public void forEach(BiConsumer<Path, LookupData> biConsumer) {
        cache.forEach((path, cacheEntry) -> {
            biConsumer.accept(path, cacheEntry.lookupData.get());
        });
    }

    public int size() {
        return cache.size();
    }

    /**
     * submit job to reap an expired cache exactly once
     */
    public void reapExpired() {
        try {
            if (cache.size() > cacheSize) {
                log.info("Reaping {} write cache entries", cache.size() - cacheSize);
                expireStream(cache
                        .entrySet()
                        .stream()
                        .sorted(Comparator.comparing(entry -> entry.getValue().lastTouched.get()))
                        .skip(cacheSize)
                        .filter(entry -> {
                            log.info("expiring {}", entry.getValue().lastTouched.get());
                            return true;
                        })
                );
            }

        } finally {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                log.error("Reaper sleep interrupted");
            }

            AutoFlusher.flushExecPool.submit(this::reapExpired);
        }
    }

    public void expireStream(Stream<Map.Entry<Path, CacheEntry>> stream) {
        ArrayList<Future> futures = new ArrayList<>();

        stream.forEach(cacheEntry -> {
            if (cacheEntry.getValue().tombStone.compareAndSet(false, true)) {
                futures.add(AutoFlusher.flushExecPool.submit(expire(cacheEntry.getKey())));
            }
        });
        Futures.getAll(futures);
    }


    public Runnable expire(Path path) {
        return () -> {
            cache.compute(path, (keyPath, cacheEntry) -> {

                try {
                    cacheEntry.lookupData.get().close();
                } catch (IOException e) {
                    log.error("Could not close LookupData for path {}", path);
                } catch (Exception e) {
                    log.error("Unexpected exception while closing {}", path);
                }

                return null;
            });
        };
    }

}
