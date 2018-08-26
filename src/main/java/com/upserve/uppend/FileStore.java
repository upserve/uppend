package com.upserve.uppend;

import com.google.common.hash.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

abstract class FileStore<T> implements AutoCloseable, RegisteredFlushable, Trimmable {
    static final int MAX_NUM_PARTITIONS = 9999;

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final Path dir;
    final Path partitionsDir;

    private final int flushDelaySeconds;
    final ConcurrentHashMap<String, T> partitionMap;

    protected final boolean readOnly;
    protected final String name;
    private final Path lockPath;
    private final FileChannel lockChan;
    private final FileLock lock;
    private final int partitionSize;
    private final boolean doHashPartitionValues;

    final AtomicBoolean isClosed;

    private static final int PARTITION_HASH_SEED = 626433832;
    private final HashFunction hashFunction = Hashing.murmur3_32(PARTITION_HASH_SEED);

    FileStore(Path dir, int flushDelaySeconds, int partitionSize, boolean readOnly, String name) {
        if (dir == null) {
            throw new NullPointerException("null dir");
        }
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }
        partitionsDir = dir.resolve("partitions");
        if (partitionSize > MAX_NUM_PARTITIONS) {
            throw new IllegalArgumentException("bad partition size: greater than max (" + MAX_NUM_PARTITIONS + "): " + partitionSize);
        }
        if (partitionSize < 0) {
            throw new IllegalArgumentException("bad partition size: negative: " + partitionSize);
        }
        this.partitionSize = partitionSize;
        if (partitionSize == 0) {
            partitionMap = new ConcurrentHashMap<>();
            doHashPartitionValues = false;
        } else {
            partitionMap = new ConcurrentHashMap<>(partitionSize);
            doHashPartitionValues = true;
        }
        this.name = name;

        this.flushDelaySeconds = flushDelaySeconds;
        if (!readOnly && flushDelaySeconds > 0) register(flushDelaySeconds);

        this.readOnly = readOnly;
        lockPath = readOnly ? dir.resolve("readLock") : dir.resolve("writeLock");

        try {
            lockChan = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            lock = readOnly ? lockChan.lock(0L, Long.MAX_VALUE, true) : lockChan.lock(); // Write lock is exclusive
        } catch (IOException e) {
            throw new UncheckedIOException("unable to open lock: " + lockPath, e);
        } catch (OverlappingFileLockException e) {
            throw new IllegalStateException("lock busy: " + lockPath, e);
        }

        isClosed = new AtomicBoolean(false);
    }

    String partitionHash(String partition) {
        if (doHashPartitionValues) {
            HashCode hcode = hashFunction.hashBytes(partition.getBytes(StandardCharsets.UTF_8));
            return String.format("%04d", Math.abs(hcode.asInt()) % partitionSize);
        } else {
            return partition;
        }
    }

    abstract Function<String, T> getOpenPartitionFunction();

    abstract Function<String, T> getCreatePartitionFunction();

    Optional<T> getIfPresent(String partitionEntropy) {
        return Optional.ofNullable(partitionMap.computeIfAbsent(
                partitionHash(partitionEntropy),
                getOpenPartitionFunction()
        ));
    }

    T getOrCreate(String partitionEntropy) {
        return partitionMap.computeIfAbsent(
                partitionHash(partitionEntropy),
                getCreatePartitionFunction()
        );
    }

    Stream<T> streamPartitions() {
        try {
            Files
                    .list(partitionsDir)
                    .map(path -> path.toFile().getName())
                    .forEach(partition -> partitionMap.computeIfAbsent(
                            partition,
                            getOpenPartitionFunction()
                    ));
        } catch (NoSuchFileException e) {
            log.debug("Partitions directory does not exist: {}", partitionsDir);
            return Stream.empty();

        } catch (IOException e) {
            log.error("Unable to list partitions in " + partitionsDir, e);
            return Stream.empty();
        }
        return partitionMap.values().parallelStream();
    }


    protected abstract void flushInternal() throws IOException;

    protected abstract void closeInternal() throws IOException;

    protected abstract void trimInternal() throws IOException;

    @Override
    public void trim() {
        log.debug("Triming {}", name);
        try {
            trimInternal();
        } catch (Exception e) {
            log.error("unable to trim {}", name, e);
        }
        log.debug("Trimed {}", name);
    }

    @Override
    public void register(int seconds) {
        AutoFlusher.register(seconds, this);
    }

    @Override
    public void deregister() {
        AutoFlusher.deregister(this);
    }

    @Override
    public void flush() {
        log.info("flushing {}", name);
        try {
            flushInternal();
        } catch (Exception e) {
            log.error("unable to flush {}", name, e);
        }
        log.info("flushed {}", name);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            log.warn("close called twice on file store: " + name);
            return;
        }

        if (!readOnly && flushDelaySeconds > 0) AutoFlusher.deregister(this);

        try {
            closeInternal();
        } catch (Exception e) {
            log.error("unable to close {}", name, e);
        }

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
    }
}
