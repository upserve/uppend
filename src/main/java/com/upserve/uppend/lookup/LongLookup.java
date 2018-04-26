package com.upserve.uppend.lookup;

import com.google.common.hash.*;
import com.upserve.uppend.AutoFlusher;
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
     * hash size is the number of hash elements per partition. Key
     * values hashed and modded by this number will be represented as paths
     * of hexits representing the value, where each two hexits of a prefix
     * will be a directory and the final one or two hexits will be a file.
     */
    private static final int MAX_HASH_SIZE = 1 << 24; /* 16,777,216 */

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
     * @return the long value for the key, or null if not found
     */
    public Long getLookupData(String key) {
        log.trace("getting from {}: {}", lookupsDir, key);
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).get(lookupKey);
    }


    /**
     * Set the value of this key.
     *
     * @param key the key to check or set
     * @param value the value to set for this key
     * @return the previous value associated with the key or null if it did not exist
     */
    public Long put(String key, long value) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).put(lookupKey, value);
    }

    /**
     * Set the value of the key if it does not exist and return the new value. If it does exist, return the existing value.
     *
     * @param key the key to check or set
     * @param allocateLongFunc the function to call to getLookupData the value if this is a new key
     * @return the value associated with the key
     */
    public long putIfNotExists(String key, LongSupplier allocateLongFunc) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);

        return getLookupData(hashPath).putIfNotExists(lookupKey, allocateLongFunc);
    }

    /**
     * Increment the value associated with this key by the given amount
     *
     * @param key the key to be incremented
     * @param delta the amount to increment the value by
     * @return the value new associated with the key
     */
    public long increment(String key, long delta) {
        LookupKey lookupKey = new LookupKey(key);
        Path hashPath = hashPath(lookupKey);
        return getLookupData(hashPath).increment(lookupKey, delta);
    }

    private LookupData getLookupData(Path hashPath) {
        return lookups
                .computeIfAbsent(
                        hashPath,
                        pathKey -> new LookupData(pathKey, partitionLookupCache)
                );
    }


    private Optional<LookupData> safeGetLookupData(Path hashPath){
        if (readOnly) {
            LookupData result = lookups.get(hashPath);
            if (result == null && Files.exists(LookupData.metadataPath(hashPath))){
                result = getLookupData(hashPath);
            }
            return Optional.ofNullable(result);

        } else {
            return Optional.of(getLookupData(hashPath));
        }
    }

    public Stream<String> keys() {
//        return hashPaths()
//                .map(this::safeGetLookupData)
//                .filter(Optional::isPresent)
//                .map(Optional::get)
//                .flatMap(LookupData::keys)
//                .map(LookupKey::string);
        return Stream.empty();

    }

    /**
     * Scan the long lookups for a given partition streaming the key and long
     *
     * @return a stream of entries of key and long value
     */
    public Stream<Map.Entry<String, Long>> scan() {
//        return hashPaths()
//                .map(this::safeGetLookupData)
//                .filter(Optional::isPresent)
//                .map(Optional::get)
//                .flatMap(LookupData::scan);
        return Stream.empty();
    }

    /**
     * Scan a partition and call a function for each key and value
     *
     * @param keyValueFunction function to call for each key and long value
     */
    public void scan(ObjLongConsumer<String> keyValueFunction) {
//        hashPaths()
//                .map(this::safeGetLookupData)
//                .filter(Optional::isPresent)
//                .map(Optional::get)
//                .forEach(lookupData -> {
//                    lookupData.scan(keyValueFunction);
//                });
    }


    @Override
    public void flush() {

        lookups.values().forEach(lookupData -> {
            try {
                lookupData.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to flush lookupData for: " + lookupData.getHashPath(), e);
            }
        });

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
                .mapToObj(i -> String.format("%02x", i));

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
