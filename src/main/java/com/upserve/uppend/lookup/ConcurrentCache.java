package com.upserve.uppend.lookup;

import com.google.common.collect.*;
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

    private final AtomicInteger taskCount = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final java.util.Timer cacheReaperTimer = new java.util.Timer();

    public ConcurrentCache(int cacheSize, float loadFactor) {
        this.cache = new ConcurrentHashMap<>();
        this.cacheSize = cacheSize;

        cacheReaperTimer.schedule(reaperTaskTimer(), 5000, 5000);
    }

    public void clear() {
        this.cache.clear();
    }

    public void flush() {
        ArrayList<Future> futures = new ArrayList<>();
        cache.forEach((path, entry) -> {
            if (entry.lookupData.get().isDirty()) {
                taskCount.addAndGet(1);
                futures.add(AutoFlusher.flushExecPool.submit(() -> {
                            try {
                                log.trace("cache flushing {}", path);
                                entry.lookupData.get().flush();
                                log.trace("cache flushed {}", path);
                            } catch (Exception e) {
                                log.error("unable to flush " + path, e);
                            } finally {
                                taskCount.addAndGet(-1);
                            }
                        }
                ));
            }
        });
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

    public void close() {
        closed.set(true);
        cacheReaperTimer.cancel();
        purge();
    }

    public void forEach(BiConsumer<Path, LookupData> biConsumer) {
        cache.forEach((path, cacheEntry) -> {
            biConsumer.accept(path, cacheEntry.lookupData.get());
        });
    }

    public int size() {
        return cache.size();
    }

    public long totalKeys() {
        return cache.entrySet().stream().mapToLong(entry -> entry.getValue().lookupData.get().size()).sum();
    }

    public int taskCount() {
        return taskCount.get();
    }


    private TimerTask reaperTaskTimer() {
        return new TimerTask() {
            @Override
            public void run() {
                if (cache.size() > cacheSize * 1.5) {
                    log.debug("Reaping {} write cache entries", cache.size() - cacheSize);
                    expireStream(cache
                            .entrySet()
                            .stream()
                            .map(entry -> Maps.immutableEntry(entry.getValue().lastTouched.get(), entry))
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .skip(cacheSize)
                            .map(Map.Entry::getValue)
                    );
                }
            }
        };
    }

    public void expireStream(Stream<Map.Entry<Path, CacheEntry>> stream) {
        ArrayList<Future> futures = new ArrayList<>();

        stream.forEach(cacheEntry -> {
            if (cacheEntry.getValue().tombStone.compareAndSet(false, true)) {
                taskCount.addAndGet(1);
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
                } finally {
                    taskCount.addAndGet(-1);
                }

                return null;
            });
        };
    }

}
