package com.upserve.uppend;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class AutoFlusher {
    private static final int FLUSH_EXEC_POOL_NUM_THREADS = 20;

    private static final ConcurrentMap<Flushable, Integer> flushableDelays = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, ConcurrentLinkedQueue<Flushable>> delayFlushables = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, ScheduledFuture> delayFutures = new ConcurrentHashMap<>();

    private static final ThreadFactory threadFactory;
    private static final ExecutorService flushExecPool;

    static {
        ThreadGroup serverThreadGroup = new ThreadGroup("auto-flush");
        AtomicInteger threadNumber = new AtomicInteger();
        threadFactory = r -> new Thread(serverThreadGroup, r, "auto-flush-" + threadNumber.incrementAndGet());

        AtomicInteger flushExecPoolThreadNumber = new AtomicInteger();
        ThreadFactory flushExecPoolThreadFactory = r -> new Thread(serverThreadGroup, r, "auto-flush-exec-pool-" + flushExecPoolThreadNumber.incrementAndGet());
        flushExecPool = Executors.newFixedThreadPool(FLUSH_EXEC_POOL_NUM_THREADS, flushExecPoolThreadFactory);
    }

    public static void register(int delaySeconds, Flushable flushable) {
        Integer existingDelay = flushableDelays.put(flushable, delaySeconds);
        if (existingDelay != null) {
            throw new IllegalStateException("flushable already registered: " + flushable);
        }

        ConcurrentLinkedQueue<Flushable> flushables = delayFlushables.computeIfAbsent(delaySeconds, delaySeconds2 -> {
            synchronized (delayFutures) {
                delayFutures.computeIfAbsent(delaySeconds2, delaySeconds3 ->
                        Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleWithFixedDelay(
                                () -> AutoFlusher.flush(delaySeconds),
                                delaySeconds,
                                delaySeconds,
                                TimeUnit.SECONDS
                        )
                );
            }
            return new ConcurrentLinkedQueue<>();
        });

        flushables.add(flushable);
    }

    public static void deregister(Flushable flushable) {
        Integer delaySeconds = flushableDelays.remove(flushable);
        if (delaySeconds == null) {
            throw new IllegalStateException("unknown flushable (flushable delays): " + flushable);
        }
        ConcurrentLinkedQueue<Flushable> flushables = delayFlushables.get(delaySeconds);
        if (flushables == null) {
            throw new IllegalStateException("unknown delay: " + delaySeconds);
        }
        if (!flushables.remove(flushable)) {
            log.warn("unknown flushable (delay flushables): " + flushable);
        }
    }

    private static void flush(int delaySeconds) {
        log.info("flushing {}", delaySeconds);
        try {
            ConcurrentLinkedQueue<Flushable> flushables = delayFlushables.get(delaySeconds);
            if (flushables == null) {
                log.error("got null flushables set for delay: " + delaySeconds);
            } else {
                ConcurrentLinkedQueue<Flushable> errorFlushables = new ConcurrentLinkedQueue<>();
                ArrayList<Future> futures = new ArrayList<>();
                for (Flushable flushable : flushables) {
                    futures.add(flushExecPool.submit(() -> {
                        try {
                            flushable.flush();
                        } catch (IOException e) {
                            log.error("unable to flush " + flushable, e);
                            errorFlushables.add(flushable);
                        }
                    }));
                }
                futures.forEach(f -> {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        log.error("interrupted while flushing", e);
                        Thread.interrupted();
                    } catch (ExecutionException e) {
                        log.error("exception executing flush", e);
                    }
                });
                flushables.removeAll(errorFlushables);
            }
        } catch (Exception e) {
            log.error("error during auto-flush", e);
        }
        log.info("flushed {}", delaySeconds);
    }
}
