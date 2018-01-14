package com.upserve.uppend.lookup;

import com.google.common.base.Charsets;
import com.google.common.hash.*;
import com.upserve.uppend.AutoFlusher;
import com.upserve.uppend.util.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public class LongLookup implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
    private final ConcurrentCache writeCache;

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
        if (writeCacheSize < 0) {
            throw new IllegalArgumentException("writeCacheSize must be >= 0");
        }

        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        if (hashSize == 1) {
            hashBytes = 1;
            hashFinalByteMask = 0;
            hashFunction = null;
        } else {
            String hashBinaryString = Integer.toBinaryString(hashSize - 1);
            hashBytes = (hashBinaryString.length() + 7) / 8;
            int hashLastByteBits = hashBinaryString.length() % 8;
            if (hashLastByteBits == 0) {
                hashLastByteBits = 8;
            }
            hashFinalByteMask = (1 << hashLastByteBits) - 1;
            hashFunction = Hashing.murmur3_32();
        }

        if (writeCacheSize == 0) {
            writeCache = null;
        } else {
            writeCache = new ConcurrentCache(writeCacheSize + 1, 1.1f);
        }

    }

    /**
     * Get the value associated with the given partition and key, consulting
     * the write cache of this {@code LongLookup} so that unflushed values are
     * visible
     *
     * @param partition the partition to look up
     * @param key       the key to look up
     * @return the value for the partition and key, or -1 if not found
     */
    public long get(String partition, String key) {
        log.trace("getting from {}: {}: {}", dir, partition, key);

        validatePartition(partition);

        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(partition, lookupKey);

        if (writeCache != null) {
            Long value = writeCache.evaluateIfPresent(hashPath, lookupData -> lookupData.get(lookupKey));
            if (value != null) {
                return value == Long.MIN_VALUE ? -1 : value;
            }
        }

        return getUncachedInternal(lookupKey, hashPath);
    }

    /**
     * Get the value associated with the given partition and key, without
     * consulting the write cache of this {@code LongLookup} so that unflushed
     * values are not visible
     *
     * @param partition the partition to look up
     * @param key       the key to look up
     * @return the value for the partition and key, or -1 if not found
     */
    public long getFlushed(String partition, String key) {
        log.trace("getting uncached from {}: {}: {}", dir, partition, key);

        validatePartition(partition);

        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(partition, lookupKey);

        return getUncachedInternal(lookupKey, hashPath);
    }

    /**
     * Scan the long lookups for a given partition streaming the key and long
     * @param partition the partition name to scan
     * @return a Stream of entries containing key and long
     */
    public Stream<Map.Entry<String, Long>> scan(String partition) {
        validatePartition(partition);

        return hashPaths(partition).flatMap(LookupData::scan);
    }

    public int cacheSize(){
        return writeCache.size();
    }

    public long put(String partition, String key, long value) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(partition, lookupKey);
        return loadFromCacheForWrite(hashPath, lookupData -> lookupData.put(lookupKey, value));
    }

    public long putIfNotExists(String partition, String key, LongSupplier allocateLongFunc) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(partition, lookupKey);
        return loadFromCacheForWrite(hashPath, lookupData -> lookupData.putIfNotExists(lookupKey, allocateLongFunc));
    }

    public long increment(String partition, String key, long delta) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(partition, lookupKey);
        return loadFromCacheForWrite(hashPath, lookupData -> lookupData.increment(lookupKey, delta));
    }

    public Stream<String> keys(String partition) {
        validatePartition(partition);
        return hashPaths(partition)
                .flatMap(LookupData::keys)
                .map(LookupKey::string);
    }

    public Stream<String> partitions() {
        File[] files = dir.toFile().listFiles();
        if (files == null) {
            return Stream.empty();
        }

        return Arrays
                .stream(files)
                .filter(File::isDirectory)
                .map(File::getName);
    }

    public LongStream size(String partition){
        validatePartition(partition);
        return hashPaths(partition)
                .mapToLong(LookupData::size);
    }

    public void scan(String partition, BiConsumer<String, Long> keyValueFunction) {
        validatePartition(partition);

        hashPaths(partition)
                .forEach(p -> {
                    LookupData.scan(p, keyValueFunction);
                });
    }

    @Override
    public void close() {
        if (writeCache != null) {
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
                }

            });
            writeCache.clear();
        }
        log.trace("closed {}", dir);
    }

    @Override
    public void flush() {
        if (writeCache == null) {
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("flushing {}", dir);
        }

        ArrayList<Future> futures = new ArrayList<>();
        writeCache.forEach((path, data) ->
                futures.add(AutoFlusher.flushExecPool.submit(() -> {
                            try {
                                log.trace("cache flushing {}", path);
                                data.flush();
                                log.trace("cache flushed {}", path);
                            } catch (Exception e) {
                                log.error("unable to flush " + path, e);
                            }
                        }
                )));
        Futures.getAll(futures);
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

    private Long loadFromCacheForWrite(Path hashPath, Function<LookupData, Long> function) {
        if (writeCache == null) {
            throw new IllegalStateException("attempting write without a write cache: " + hashPath);
        }
        return writeCache.compute(hashPath, function);
    }

    public Path hashPath(String partition, LookupKey key) {
        String hashPath = hashFunction == null ? "00" : hashPath(hashFunction.hashString(key.string(), Charsets.UTF_8));
        return dir.resolve(partition).resolve(hashPath);
    }

    String hashPath(HashCode hashCode) {
        // Also see keys method in this class
        String hashPath;
        byte[] hash = hashCode.asBytes();
        switch (hashBytes) {
            case 1:
                hashPath = String.format("%02x", hashFinalByteMask & (int) hash[0]);
                break;
            case 2:
                hashPath = String.format("%02x/%02x", 0xff & (int) hash[0], hashFinalByteMask & (int) hash[1]);
                break;
            case 3:
                hashPath = String.format("%02x/%02x/%02x", 0xff & (int) hash[0], 0xff & (int) hash[1], hashFinalByteMask & (int) hash[2]);
                break;
            default:
                throw new IllegalStateException("unhandled hashBytes: " + hashBytes);
        }
        return hashPath;
    }

    private Stream<Path> hashPaths(String partition) {
        validatePartition(partition);

        Path partitionPath = dir.resolve(partition);
        if (Files.notExists(partitionPath)) {
            return Stream.empty();
        }

        // Also see hashPath method in this class
        Stream<String> paths = IntStream
                .range(0, hashFinalByteMask + 1)
                .mapToObj(i -> String.format("%02x/data", i));

        for (int i = 1; i < hashBytes; i++) {
            paths = paths
                    .flatMap(child -> IntStream
                            .range(0, 256)
                            .mapToObj(j -> String.format("%02x/%s", j, child))
                    );
        }

        return paths
                .map(partitionPath::resolve)
                .filter(Files::exists);
    }

    private long getUncachedInternal(LookupKey lookupKey, Path hashPath) {
        Path metaPath = hashPath.resolve("meta");
        if (!Files.exists(metaPath)) {
            log.trace("no metadata for key {} at path {}", lookupKey, metaPath);
            return -1;
        }

        LookupMetadata metadata = new LookupMetadata(metaPath);
        return metadata.readData(hashPath.resolve("data"), lookupKey);
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
