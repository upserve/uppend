package com.upserve.uppend;

import com.google.common.hash.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.math.IntMath.mod;

abstract class FileStore<T> implements AutoCloseable, RegisteredFlushable, Trimmable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final Path dir;

    private final int flushDelaySeconds;
    protected final Map<String, T> partitionMap;

    protected final boolean readOnly;
    protected final String name;
    private final Path lockPath;
    private final FileChannel lockChan;
    private final FileLock lock;
    protected final int partitionSize;
    private final boolean hashPartitionValues;

    protected final AtomicBoolean isClosed;

    private static final int PARTITION_HASH_SEED = 626433832;
    private final HashFunction hashFunction = Hashing.murmur3_32(PARTITION_HASH_SEED);


    FileStore(Path dir, int flushDelaySeconds, int partitionSize, boolean readOnly, String name) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }
        this.partitionSize = partitionSize;
        if (partitionSize > 0) {
            if (partitionSize > 9999) throw new IllegalArgumentException("Partition size is greater than 9999");
            hashPartitionValues = true;
            partitionMap = new ConcurrentHashMap<>(partitionSize);
        } else {
            partitionMap = new ConcurrentHashMap<>();
            hashPartitionValues = false;
        }
        this.name = name;

        this.flushDelaySeconds = flushDelaySeconds;

        this.readOnly = readOnly;
        if (!readOnly) {
            lockPath = dir.resolve("lock");

            if (flushDelaySeconds > 0) register(flushDelaySeconds);

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

        isClosed = new AtomicBoolean(false);
    }

    protected String partitionHash(String partition) {
        if (hashPartitionValues) {
            HashCode hcode = hashFunction.hashBytes(partition.getBytes(StandardCharsets.UTF_8));
            return String.format("%04d", mod(hcode.asInt(), partitionSize));
        } else {
            return partition;
        }
    }

    protected static Path partitionPath(Path dir) {
        return dir.resolve("partitions");
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

    Stream<T> streamPartitions(Path partitiondPath) {
        try {
            Files
                    .list(partitiondPath)
                    .map(path -> path.toFile().getName())
                    .forEach(partition -> partitionMap.computeIfAbsent(
                            partition,
                            getOpenPartitionFunction()
                    ));
        } catch (NoSuchFileException e) {
            log.debug("Partitions director does not exist: {}", partitiondPath);
            return Stream.empty();

        } catch (IOException e) {
            log.error("Unable to list partitions in {}", partitiondPath, e);
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

        if (!readOnly) {
            if (flushDelaySeconds > 0) AutoFlusher.deregister(this);
        }

        try {
            closeInternal();
        } catch (Exception e) {
            log.error("unable to close {}", name, e);
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
