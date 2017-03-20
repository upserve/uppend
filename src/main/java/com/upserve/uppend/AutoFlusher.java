package com.upserve.uppend;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class AutoFlusher {
    private static final Map<Flushable, Integer> flushableDelays = new HashMap<>();
    private static final Map<Integer, Set<Flushable>> delayFlushables = new HashMap<>();
    private static final Map<Integer, ScheduledFuture> delayFutures = new HashMap<>();
    private static final ThreadFactory threadFactory;

    static {
        ThreadGroup serverThreadGroup = new ThreadGroup("auto-flush");
        AtomicInteger threadNumber = new AtomicInteger();
        threadFactory = r -> new Thread(serverThreadGroup, r, "auto-flush-" + threadNumber.incrementAndGet());
    }

    public static synchronized void register(int delaySeconds, Flushable flushable) {
        Integer existingDelay = flushableDelays.put(flushable, delaySeconds);
        if (existingDelay != null) {
            throw new ConcurrentModificationException("unexpected race in synchronized access to flushable delays: " + delaySeconds);
        }
        Set<Flushable> flushables = delayFlushables.get(delaySeconds);
        if (flushables == null) {
            ScheduledFuture future = Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleWithFixedDelay(
                    () -> AutoFlusher.flush(delaySeconds),
                    delaySeconds,
                    delaySeconds,
                    TimeUnit.SECONDS
            );
            ScheduledFuture existingFuture = delayFutures.put(delaySeconds, future);
            if (existingFuture != null) {
                throw new ConcurrentModificationException("unexpected race in synchronized access to delay futures: " + delaySeconds);
            }
            flushables = new HashSet<>();
            Set<Flushable> existingFlushables = delayFlushables.put(delaySeconds, flushables);
            if (existingFlushables != null) {
                throw new ConcurrentModificationException("unexpected race in synchronized access to delay flushables: " + delaySeconds);
            }
        }
        flushables.add(flushable);
    }

    public static synchronized void deregister(Flushable flushable) {
        Integer delaySeconds = flushableDelays.remove(flushable);
        if (delaySeconds == null) {
            throw new IllegalStateException("unknown flushable (flushable delays): " + flushable);
        }
        Set<Flushable> flushables = delayFlushables.get(delaySeconds);
        if (flushables == null) {
            throw new IllegalStateException("unknown delay: " + delaySeconds);
        }
        if (!flushables.remove(flushable)) {
            throw new IllegalStateException("unknown flushable (delay flushables): " + flushable);
        }
        if (flushables.isEmpty()) {
            if (delayFlushables.remove(delaySeconds) != flushables) {
                throw new ConcurrentModificationException("flushables list changed inside within synchronized access");
            }
            ScheduledFuture future = delayFutures.remove(delaySeconds);
            future.cancel(false);
        }
    }

    private static synchronized void flush(int delaySeconds) {
        try {
            Set<Flushable> errorFlushables = null;
            Set<Flushable> flushables = delayFlushables.get(delaySeconds);
            if (flushables == null) {
                log.error("got null flushables set for delay: " + delaySeconds);
            }
            for (Flushable flushable : flushables) {
                try {
                    flushable.flush();
                } catch (IOException e) {
                    log.error("unable to flush " + flushable, e);
                    if (errorFlushables == null) {
                        errorFlushables = new HashSet<>();
                    }
                    errorFlushables.add(flushable);
                }
            }
            if (errorFlushables != null) {
                flushables.removeAll(errorFlushables);
            }
        } catch (Exception e) {
            log.error("error during auto-flush", e);
        }
    }
}
