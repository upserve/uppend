package com.upserve.uppend.lookup;

import com.google.common.hash.*;
import com.upserve.uppend.util.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class LongLookup implements Flushable {
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

    private final Path lookupsDir;
    private final boolean readOnly;
    private final int hashBytes;
    private final int hashFinalByteMask;
    private final HashFunction hashFunction;

    private final ConcurrentHashMap<Path, LookupData> lookups;

    private final PartitionLookupCache partitionLookupCache;

    public LongLookup(Path lookupsDir, int hashSize, PartitionLookupCache partitionLookupCache) {
        if (hashSize < 1) {
            throw new IllegalArgumentException("hashSize must be >= 1");
        }
        if (hashSize > MAX_HASH_SIZE) {
            throw new IllegalArgumentException("hashSize must be <= " + MAX_HASH_SIZE);
        }

        this.readOnly = partitionLookupCache.getPageCache().readOnly();
        this.lookupsDir = lookupsDir;
        this.partitionLookupCache = partitionLookupCache;

        lookups = new ConcurrentHashMap<>();

        try {
            Files.createDirectories(lookupsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + lookupsDir, e);
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
    }

    /**
     * Get the value associated with the given partition and key, consulting
     * the write cache of this {@code LongLookup} so that unflushed values are
     * visible
     *
     * @param key the key to look up
     * @return the long value for the key, or @{code Long.MIN_VALUE} if not found
     */
    public long getLookupData(String key) {
        log.trace("getting from {}: {}", lookupsDir, key);
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).get(lookupKey);
        }

    public long put(String key, long value) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).put(lookupKey, value);

    }

    public long putIfNotExists(String key, LongSupplier allocateLongFunc) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);

        return getLookupData(hashPath).putIfNotExists(lookupKey, allocateLongFunc);
    }

    public long increment(String key, long delta) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).increment(lookupKey, delta);
    }

    private Optional<LookupData> getLookupData(Path hashPath){
        if (readOnly) {
            LookupData result = lookups.get(hashPath);
            if (result == null && Files.exists(LookupData.metadataPath(hashPath))){
                result = lookups
                        .computeIfAbsent(
                                hashPath,
                                pathKey -> new LookupData(pathKey, partitionLookupCache)
                        );
            }
            return Optional.ofNullable(result);

        } else {
            return Optional.of(lookups
                    .computeIfAbsent(
                            hashPath,
                            pathKey -> new LookupData(pathKey, partitionLookupCache)
                    )
            );
        }
    }

    public Stream<String> keys() {
        return hashPaths()
                .map(this::getLookupData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(LookupData::keys)
                .map(LookupKey::string);
    }

    /**
     * Scan the long lookups for a given partition streaming the key and long
     *
     * @return a stream of entries of key and long value
     */
    public Stream<Map.Entry<String, Long>> scan() {
        return hashPaths().map(this::getLookupData).flatMap(LookupData::scan);
    }

    /**
     * Scan a partition and call a function for each key and value
     *
     * @param keyValueFunction function to call for each key and long value
     */
    public void scan(ObjLongConsumer<String> keyValueFunction) {
        hashPaths()
                .map(this::getLookupData)
                .forEach(lookupData -> {
                    lookupData.scan(keyValueFunction);
                });
    }


    @Override
    public void flush() throws IOException{
        for (LookupData lookup: lookups.values()){
            lookup.flush();
        }
        log.trace("flushed {}", lookupsDir);
    }

    public void clear() {
        log.info("clearing {}", lookupsDir);

        lookups.clear();
        try {
            if (Files.exists(lookupsDir)) {
                Path tmpDir = Files.createTempFile(lookupsDir.getParent(), lookupsDir.getFileName().toString(), ".defunct");
                Files.delete(tmpDir);
                Files.move(lookupsDir, tmpDir);
                SafeDeleting.removeDirectory(tmpDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable to delete lookups: " + lookupsDir, e);
        }
    }

    protected Path hashPath(LookupKey key) {
        String hashPath = hashFunction == null ? "00" : hashPath(hashFunction.hashBytes(key.bytes()));
        return lookupsDir.resolve(hashPath);
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

    private Stream<Path> hashPaths() {
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
                .map(lookupsDir::resolve)
                .filter(Files::exists);
    }
}
