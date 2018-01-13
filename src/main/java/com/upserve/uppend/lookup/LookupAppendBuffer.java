package com.upserve.uppend.lookup;

import com.google.common.collect.*;
import com.upserve.uppend.BlockedLongs;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * A buffered Lookup Writer for memory efficient writes to random hashPaths in the append only store.
 * This buffer maintains a high throughput with fixed memory use by sacrificing guaranteed durability. The application
 * must successfully flush the append store before an append is guaranteed to be written.
 */
public class LookupAppendBuffer {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MIN_BUFFER_SIZE = 20;

    private final int maxSize;
    private final int minSize;
    private final LongLookup longLookup;
    private final BlockedLongs blockedLongs;
    private final ConcurrentHashMap<Path, List<Map.Entry<LookupKey, Long>>> appendBuffer;
    private final ConcurrentHashMap<Path, AtomicBoolean> lockedHashPaths;
    private final ExecutorService threadPool;
    private final boolean myThreadPool;

    private final ConcurrentLinkedQueue<Future> tasks = Queues.newConcurrentLinkedQueue();
    AtomicBoolean closed = new AtomicBoolean(false);

    public LookupAppendBuffer(LongLookup longLookup, BlockedLongs blockedLongs, int bufferMax, int buffRange, Optional<ExecutorService> threadPool) {
        if (bufferMax < MIN_BUFFER_SIZE)
            throw new IllegalArgumentException(String.format("Invalid buffer size %s is less than %s", bufferMax, MIN_BUFFER_SIZE));
        this.longLookup = longLookup;
        this.blockedLongs = blockedLongs;
        this.appendBuffer = new ConcurrentHashMap<>();
        this.lockedHashPaths = new ConcurrentHashMap<>();
        this.minSize = bufferMax;
        this.maxSize = bufferMax + buffRange +1;
        this.threadPool = threadPool.orElse(Executors.newFixedThreadPool(4));
        this.myThreadPool = !threadPool.isPresent();

        this.threadPool.submit(() -> cleanupTask());
    }


    /**
     * Add a blob position for a partition and key the the buffered Map. If the number of entries for that hashPath
     * exceeds the bufferSize then submit a task to flush that entry.
     *
     * @param partition the append store partition
     * @param key       the append store key
     * @param blobPos   the position of the bytes in the blob file
     */
    public void bufferedAppend(String partition, String key, long blobPos) {
        LookupKey lookupKey = new LookupKey(key);

        // ConcurrentHashMap guarantees atomic, blocking exactly once execution of the compute method.
        appendBuffer.compute(longLookup.hashPath(partition, lookupKey), (Path pathKey, List<Map.Entry<LookupKey, Long>> entryList) -> {
            if (entryList == null) {
                entryList = new ArrayList<>(maxSize);
            }

            if (closed.get()) throw new RuntimeException("Closed for business");

            entryList.add(Maps.immutableEntry(lookupKey, blobPos));

            if (entryList.size() >= ThreadLocalRandom.current().nextInt(minSize, maxSize)) {
                List<Map.Entry<LookupKey, Long>> finalEntryList = new ArrayList<>(entryList);
                entryList.clear();
                tasks.add(threadPool.submit(() -> flushEntry(pathKey, finalEntryList)));
            }
            return entryList;
        });
    }

    /**
     * Returns the size of the buffer Map. Note that entries are never removed so the map will grow to the size of
     * the hash space and never shrink. This is not the number of appends currently buffered.
     *
     * @return the number of entries in the buffer Map.
     */
    public int bufferCount() {
        return appendBuffer.size();
    }

    protected ConcurrentLinkedQueue<Future> getTasks() {
        return tasks;
    }

    private void cleanupTask() {
        AtomicLong counter = new AtomicLong();
        Iterator<Future> iter = tasks.iterator();
        iter.forEachRemaining(future -> {
            if (future.isDone()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    log.error("Buffered Append task interrupted", e);
                } catch (ExecutionException e) {
                    log.error("Buffered Append exception", e);
                } finally {
                    iter.remove();
                    counter.addAndGet(1);
                }
            }
        });

        log.debug("Reaped {} successful buffer flush tasks", counter.get());

        try {
            Thread.sleep(500);
            if (!closed.get()) threadPool.submit(this::cleanupTask);
        } catch (InterruptedException e) {
            log.error("cleanup sleep interrupted", e);
        }
    }


    public void flush() {
        appendBuffer.forEach((path, entryList) -> {
            List<Map.Entry<LookupKey, Long>> finalEntryList = new ArrayList<>(entryList);
            entryList.clear();
            tasks.add(threadPool.submit(() -> flushEntry(path, finalEntryList)));
        });

        // Ensure that all the tasks currently in the queue finish before returning
        tasks.iterator().forEachRemaining(f -> {
            try {
                f.get(30_000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("Buffered Append task interrupted", e);
            } catch (ExecutionException e) {
                log.error("Buffered Append exception", e);
            } catch (TimeoutException e) {
                log.error("Buffered Append task timed out in flush", e);
            }
        });
    }

    public void close() {
        if (closed.getAndSet(true)) return;

        flush();
        if (myThreadPool) {
            threadPool.shutdown();
            try {
                boolean result = threadPool.awaitTermination(20_000, TimeUnit.MILLISECONDS);
                if (result) {
                    log.debug("flushed the buffer and closed the thread pool!");
                } else {
                    log.error("Close timed out waiting for tasks to finish");
                }
            } catch (InterruptedException e) {
                log.error("close interrupted!", e);
            }
        }
    }

    private void flushEntry(Path path, List<Map.Entry<LookupKey, Long>> entryList) {

        AtomicBoolean locked = lockedHashPaths.computeIfAbsent(path, pathKey -> new AtomicBoolean(false));

        if (locked.compareAndSet(false, true)) {
            try {
                LookupData lookupData = new LookupData(path.resolve("data"), path.resolve("meta"));

                entryList.forEach(entry -> {
                    long blockPos = lookupData.putIfNotExists(entry.getKey(), blockedLongs::allocate);
                    blockedLongs.append(blockPos, entry.getValue());
                });

                try {
                    lookupData.close();
                } catch (IOException e) {
                    log.error("Could not close lookup buffer for {}", path);
                }
            } finally {
                locked.set(false);
            }
        } else {
            // Resubmit the job for someone else to try
            tasks.add(threadPool.submit(() -> flushEntry(path, entryList)));
        }
    }

    public void clearLock() {
        closed.set(true);
        flush();
    }

    public void unlock() {
        closed.set(false);
    }
}
