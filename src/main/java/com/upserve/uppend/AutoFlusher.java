package com.upserve.uppend;

import com.upserve.uppend.util.Futures;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoFlusher {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int FLUSH_EXEC_POOL_NUM_THREADS = 60;

    private static final ConcurrentMap<Flushable, Integer> flushableDelays = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, ConcurrentLinkedQueue<Flushable>> delayFlushables = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, ScheduledFuture> delayFutures = new ConcurrentHashMap<>();

    private static final ThreadFactory threadFactory;
    public static final ExecutorService flushExecPool;

    static {
        ThreadGroup threadGroup = new ThreadGroup("auto-flush");
        threadGroup.setDaemon(false);
        AtomicInteger threadNumber = new AtomicInteger();
        threadFactory = r -> {
            Thread t = new Thread(threadGroup, r, "auto-flush-" + threadNumber.incrementAndGet());
            t.setDaemon(true);
            return t;
        };

        AtomicInteger flushExecPoolThreadNumber = new AtomicInteger();
        ThreadFactory flushExecPoolThreadFactory = r -> new Thread(threadGroup, r, "auto-flush-exec-pool-" + flushExecPoolThreadNumber.incrementAndGet());
        flushExecPool = Executors.newFixedThreadPool(FLUSH_EXEC_POOL_NUM_THREADS, flushExecPoolThreadFactory);
    }

    public static synchronized void register(int delaySeconds, Flushable flushable) {
        log.info("registered delay {}: {}", delaySeconds, flushable);
        Integer existingDelay = flushableDelays.put(flushable, delaySeconds);
        if (existingDelay != null) {
            throw new IllegalStateException("flushable already registered: " + flushable);
        }

        ConcurrentLinkedQueue<Flushable> flushables = delayFlushables.computeIfAbsent(delaySeconds, delaySeconds2 -> {
            delayFutures.computeIfAbsent(delaySeconds2, delaySeconds3 ->
                    Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleWithFixedDelay(
                            () -> AutoFlusher.flush(delaySeconds),
                            delaySeconds,
                            delaySeconds,
                            TimeUnit.SECONDS
                    )
            );
            return new ConcurrentLinkedQueue<>();
        });

        flushables.add(flushable);
    }

    public static synchronized void deregister(Flushable flushable) {
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
        if (flushables.isEmpty()) {
            log.info("deregistered last flushable at delay {}, removing schedule");
            ScheduledFuture scheduledFuture = delayFutures.remove(delaySeconds);
            scheduledFuture.cancel(false);
            delayFlushables.remove(delaySeconds);
        }
        log.info("deregistered delay {}: {}", delaySeconds, flushable);
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
                Futures.getAll(futures);
                flushables.removeAll(errorFlushables);
            }
        } catch (Exception e) {
            log.error("error during auto-flush", e);
        }
        log.info("flushed {}", delaySeconds);
    }
}
