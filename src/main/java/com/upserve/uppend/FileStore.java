package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.metrics.MetricsStatsCounter;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;

abstract class FileStore<T> implements AutoCloseable, Flushable, Trimmable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final Path dir;

    private final int flushDelaySeconds;
    protected final Map<String, T> partitionMap;

    protected final boolean readOnly;
    protected final String name;
    private final Path lockPath;
    private final FileChannel lockChan;
    private final FileLock lock;

    private final AtomicBoolean isClosed;

    FileStore(Path dir, int flushDelaySeconds, boolean readOnly) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        name = dir.getFileName().toString();

        this.flushDelaySeconds = flushDelaySeconds;

        this.readOnly = readOnly;
        if (!readOnly) {
            lockPath = dir.resolve("lock");

            if (flushDelaySeconds < 0)
                throw new IllegalArgumentException("Flush delay can not be negative: " + flushDelaySeconds);
            AutoFlusher.register(flushDelaySeconds, this);

            try {
                lockChan = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                lock = lockChan.lock();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to open lock: " + lockPath, e);
            } catch (OverlappingFileLockException e) {
                throw new IllegalStateException("lock busy: " + lockPath, e);
            }
        } else {
            lockPath = null;
            lockChan = null;
            lock = null;
        }

        partitionMap = new ConcurrentHashMap<>();

        isClosed = new AtomicBoolean(false);
    }

    abstract Function<String, T> getOpenPartitionFunction();

    abstract Function<String, T> getCreatePartitionFunction();

    Optional<T> safeGet(String partition) {
        if (readOnly) {
            return getIfPresent(partition);
        } else {
            return Optional.of(getOrCreate(partition));
        }
    }

    Optional<T> getIfPresent(String partition) {
        return Optional.ofNullable(partitionMap.computeIfAbsent(
                partition,
                getOpenPartitionFunction()
        ));
    }

    T getOrCreate(String partition) {
        return partitionMap.computeIfAbsent(
                partition,
                getCreatePartitionFunction()
        );
    }


    protected abstract void flushInternal() throws IOException;

    protected abstract void closeInternal() throws IOException;

    protected abstract void trimInternal() throws IOException;

    @Override
    public void trim() {
        log.debug("Triming {}", dir);
        try {
            trimInternal();
        } catch (Exception e) {
            log.error("unable to trim {}", dir, e);
        }
        log.debug("Trimed {}", dir);
    }

    ;

    @Override
    public void flush() {
        log.info("flushing {}", dir);
        try {
            flushInternal();
        } catch (Exception e) {
            log.error("unable to flush {}", dir, e);
        }
        log.info("flushed {}", dir);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            log.warn("close called twice on file store: " + dir);
            return;
        }

        if (!readOnly) {
            AutoFlusher.deregister(this);
        }

        try {
            closeInternal();
        } catch (Exception e) {
            log.error("unable to close {}", dir, e);
        }

        if (!readOnly) {
            try {
                lock.release();
            } catch (IOException e) {
                log.error("unable to release lock file: " + lockPath, e);
            }
            try {
                lockChan.close();
            } catch (IOException e) {
                log.error("unable to close lock file: " + lockPath, e);
            }
            try {
                Files.deleteIfExists(lockPath);
            } catch (IOException e) {
                log.error("unable to delete lock file: " + lockPath, e);
            }
        }
    }
}
