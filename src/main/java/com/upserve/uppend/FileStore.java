package com.upserve.uppend;

import com.google.common.hash.*;
import com.upserve.uppend.util.SafeDeleting;
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

abstract class FileStore<T extends Partition> implements AutoCloseable, RegisteredFlushable, Trimmable {
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
            Files.createDirectories((Files.isSymbolicLink(dir) ? Files.readSymbolicLink(dir).toRealPath() : dir));
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

    @Override
    public void flush() {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        // NPE may occur because the super class is registered in the autoflusher before the constructor finishes
        if (readOnly) throw new RuntimeException("Can not flush a store opened in read only mode:" + dir);

        log.debug("Flushing!");

        ForkJoinTask task = AutoFlusher.flusherWorkPool.submit(() ->
                partitionMap.values().parallelStream().forEach(T::flush)
        );
        try {
            task.get();
        } catch (InterruptedException e) {
            log.error("Flush interrupted", e);

        } catch (ExecutionException e) {
            throw new RuntimeException("Partition map flush failed with", e);
        }

        log.debug("Flushed!");
    }

    @Override
    public void trim(){
        log.debug("Trimming!");

        ForkJoinTask task = AutoFlusher.flusherWorkPool.submit(() ->
                partitionMap.values().parallelStream().forEach(T::trim)
        );
        try {
            task.get();
        } catch (InterruptedException e) {
            log.error("Trim interrupted", e);

        } catch (ExecutionException e) {
            throw new RuntimeException("Partition map trim failed with", e);
        }

        log.debug("Trimmed!");
    }

    @Override
    public void register(int seconds) {
        AutoFlusher.register(seconds, this);
    }

    @Override
    public void deregister() {
        AutoFlusher.deregister(this);
    }

    public void clear() {
        if (readOnly) throw new RuntimeException("Can not clear a store opened in read only mode:" + name);
        log.trace("clearing");

        closePartitions();

        try {
            SafeDeleting.removeDirectory(partitionsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clear partitions directory", e);
        }
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            log.warn("close called twice on file store: " + name);
            return;
        }

        if (!readOnly && flushDelaySeconds > 0) AutoFlusher.deregister(this);

        closePartitions();

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

    private void closePartitions(){
        ForkJoinTask task = AutoFlusher.flusherWorkPool.submit(() ->
                partitionMap.values().parallelStream().forEach(partition -> {
                    try {
                        partition.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error closing store " + name, e);
                    }
                })
        );

        try {
            task.get();
        } catch (InterruptedException e) {
            log.error("Close interrupted", e);

        } catch (ExecutionException e) {
            throw new RuntimeException("Partition map close failed with", e);
        }
        partitionMap.clear();
    }
}
