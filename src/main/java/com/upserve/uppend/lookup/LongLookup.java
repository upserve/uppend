package com.upserve.uppend.lookup;

import com.google.common.base.Charsets;
import com.google.common.hash.*;
import com.upserve.uppend.util.SafeDeleting;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Phaser;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@Slf4j
public class LongLookup implements AutoCloseable, Flushable {
    /**
     * DEFAULT_HASH_SIZE is the number of hash elements per partition. Key
     * values hashed and modded by this number will be represented as paths
     * of hexits representing the value, where each two hexits of a prefix
     * will be a directory and the final one or two hexits will be a file.
     */
    public static final int DEFAULT_HASH_SIZE = 4096;
    private static final int MAX_HASH_SIZE = 1 << 24; /* 16,777,216 */

    /**
     * DEFAULT_WRITE_CACHE_SIZE is the maximum number of open
     * {@link LookupData} entries in the write cache. It should be a multiple
     * of DEFAULT_HASH_SIZE so that we can write complete partitions without
     * thrashing the cache.
     */
    @SuppressWarnings("PointlessArithmeticExpression")
    public static final int DEFAULT_WRITE_CACHE_SIZE = DEFAULT_HASH_SIZE * 1;

    private final Path dir;
    private final int hashBytes;
    private final int hashFinalByteMask;
    private final HashFunction hashFunction;
    private final Phaser lookupDataPhaser;
    private final LinkedHashMap<Path, LookupData> writeCache;

    public LongLookup(Path dir) {
        this(dir, DEFAULT_HASH_SIZE, DEFAULT_WRITE_CACHE_SIZE);
    }

    public LongLookup(Path dir, int hashSize, int writeCacheSize) {
        if (hashSize < 1) {
            throw new IllegalArgumentException("hashSize must be >= 1");
        }
        if (hashSize > MAX_HASH_SIZE) {
            throw new IllegalArgumentException("hashSize must be <= " + MAX_HASH_SIZE);
        }
        if (writeCacheSize < 1) {
            throw new IllegalArgumentException("writeCacheSize must be >= 1");
        }

        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        String hashBinaryString = Integer.toBinaryString(hashSize - 1);
        hashBytes = (hashBinaryString.length() + 7) / 8;
        hashFinalByteMask = (1 << (hashBinaryString.length() % 8)) - 1;

        hashFunction = Hashing.murmur3_32();

        lookupDataPhaser = new Phaser(1);

        writeCache = new LinkedHashMap<Path, LookupData>(writeCacheSize + 1, 1.1f, true) {
            @Override
            protected synchronized boolean removeEldestEntry(Map.Entry<Path, LookupData> eldest) {
                if (size() > writeCacheSize) {
                    Path path = eldest.getKey();
                    log.trace("cache removing {}", path);
                    try {
                        eldest.getValue().close();
                    } catch (IOException e) {
                        log.error("unable to close " + path, e);
                        throw new UncheckedIOException("unable to close " + path, e);
                    } catch (Exception e) {
                        log.error("unexpected error closing " + path, e);
                        throw e;
                    } finally {
                        lookupDataPhaser.arriveAndDeregister();
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Get the value associated with the given partition and key
     *
     * @param partition the partition to look up
     * @param key the key to look up
     * @return the value for the partition and key, or -1 if not found
     */
    public long get(String partition, String key) {
        validatePartition(partition);

        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);

        LookupData data = loadFromWriteCacheIfExists(lenPath);
        if (data != null) {
            long value = data.get(lookupKey);
            return value == Long.MIN_VALUE ? -1 : value;
        }

        Path metaPath = lenPath.resolve("meta");
        if (!Files.exists(metaPath)) {
            log.trace("no metadata for key {} at path {}", key, metaPath);
            return -1;
        }
        LookupMetadata metadata = new LookupMetadata(lenPath.resolve("meta"));
        return metadata.readData(lenPath.resolve("data"), lookupKey);
    }

    public long put(String partition, String key, long value) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        return loadFromWriteCache(lenPath).put(lookupKey, value);
    }

    public long putIfNotExists(String partition, String key, LongSupplier allocateLongFunc) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        return loadFromWriteCache(lenPath).putIfNotExists(lookupKey, allocateLongFunc);
    }

    public long increment(String partition, String key, long delta) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        return loadFromWriteCache(lenPath).increment(lookupKey, delta);

    }

    public Stream<String> keys(String partition) {
        validatePartition(partition);

        Path partitionPath = dir.resolve(partition);
        if (Files.notExists(partitionPath)) {
            return Stream.empty();
        }

        Stream<Path> files;
        try {
            files = Files.walk(partitionPath);
        } catch (IOException e) {
            throw new UncheckedIOException("could not walk partition " + partitionPath, e);
        }
        return files
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().equals("data"))
                .flatMap(p -> LookupData.keys(p, parseKeyLengthFromPath(p.getParent())))
                .map(LookupKey::string);
    }

    public Stream<String> partitions() {
        Stream<Path> files;
        try {
            if (!Files.exists(dir)) {
                return Stream.empty();
            }
            files = Files.walk(dir, 1);
        } catch (IOException e) {
            throw new UncheckedIOException("could not walk dir " + dir, e);
        }

        return files
                .filter(Files::isDirectory)
                .filter(p -> !p.equals(dir))
                .map(p -> p.getFileName().toString());
    }

    @Override
    public void close() {
        synchronized (writeCache) {
            if (log.isTraceEnabled()) {
                log.trace("closing {} (~{} entries)", dir, writeCache.size());
            }
            writeCache.forEach((path, data) -> {
                log.trace("cache removing {}", path);
                try {
                    data.close();
                } catch (IOException e) {
                    log.error("unable to close " + path, e);
                    throw new UncheckedIOException("unable to close " + path, e);
                } finally {
                    lookupDataPhaser.arriveAndDeregister();
                }

            });
            writeCache.clear();
            if (log.isTraceEnabled()) {
                log.trace("waiting for {} registered lookup data closures", lookupDataPhaser.getRegisteredParties());
            }
        }
        lookupDataPhaser.arriveAndAwaitAdvance();
        log.trace("closed {}", dir);
    }

    @Override
    public void flush() {
        synchronized (writeCache) {
            if (log.isTraceEnabled()) {
                log.trace("flushing {} (~{} entries)", dir, writeCache.size());
            }
            writeCache.forEach((path, data) -> {
                log.trace("cache flushing {}", path);
                try {
                    data.flush();
                } catch (IOException e) {
                    log.error("unable to flush " + path, e);
                    throw new UncheckedIOException("unable to flush " + path, e);
                }
            });
        }
        log.trace("flushed {}", dir);
    }

    public void clear() {
        log.info("clearing {}", dir);
        close();
        try {
            if (Files.exists(dir)) {
                Path tmpDir = Files.createTempFile(dir.getParent(), dir.getFileName().toString(), ".defunct");
                Files.delete(tmpDir);
                Files.move(dir, tmpDir);
                SafeDeleting.removeDirectory(tmpDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable to delete lookups: " + dir, e);
        }
    }

    private LookupData loadFromWriteCache(Path lenPath) {
        synchronized (writeCache) {
            return writeCache.computeIfAbsent(lenPath, path -> {
                log.trace("cache loading {}", lenPath);
                return new LookupData(
                        parseKeyLengthFromPath(lenPath),
                        lenPath.resolve("data"),
                        lenPath.resolve("meta")
                );
            });
        }
    }

    private LookupData loadFromWriteCacheIfExists(Path lenPath) {
        synchronized (writeCache) {
            return writeCache.get(lenPath);
        }
    }

    private Path hashAndLengthPath(String partition, LookupKey key) {
        log.trace("getting from {}: {}", dir, key);
        byte[] hash = hashFunction.hashString(key.string(), Charsets.UTF_8).asBytes();
        String hashPath;
        switch (hashBytes) {
            case 1:
                hashPath = String.format("%02x/%d", hashFinalByteMask & (int) hash[0], key.byteLength());
                break;
            case 2:
                hashPath = String.format("%02x/%02x/%d", 0xff & (int) hash[0], hashFinalByteMask & (int) hash[1], key.byteLength());
                break;
            case 3:
                hashPath = String.format("%02x/%02x/%02x/%d", 0xff & (int) hash[0], 0xff & (int) hash[1], hashFinalByteMask & (int) hash[2], key.byteLength());
                break;
            default:
                throw new IllegalStateException("unhandled hashBytes: " + hashBytes);
        }
        return dir.resolve(partition).resolve(hashPath);
    }

    private static int parseKeyLengthFromPath(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

    private static void validatePartition(String partition) {
        if (partition == null) {
            throw new NullPointerException("null partition");
        }
        if (partition.isEmpty()) {
            throw new IllegalArgumentException("empty partition");
        }

        if (!isValidPartitionCharStart(partition.charAt(0))) {
            throw new IllegalArgumentException("bad first-char of partition: " + partition);
        }

        for (int i = partition.length() - 1; i > 0; i--) {
            if (!isValidPartitionCharPart(partition.charAt(i))) {
                throw new IllegalArgumentException("bad char at position " + i + " of partition: " + partition);
            }
        }
    }

    private static boolean isValidPartitionCharStart(char c) {
        return Character.isJavaIdentifierPart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }
}
