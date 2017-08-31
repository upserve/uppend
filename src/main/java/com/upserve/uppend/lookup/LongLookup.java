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
public class LongLookup implements AutoCloseable {
    private static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    private static final int DEFAULT_MAX_CACHE_SIZE = 512;

    private final Path dir;
    private final int flushDelaySeconds;
    private final HashFunction hashFunction;
    private final Phaser lookupDataPhaser;
    private final LinkedHashMap<Path, LookupData> writeCache;

    public LongLookup(Path dir) {
        this(dir, DEFAULT_MAX_CACHE_SIZE, DEFAULT_FLUSH_DELAY_SECONDS);
    }

    public LongLookup(Path dir, int maxCacheSize, int flushDelaySeconds) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        this.flushDelaySeconds = flushDelaySeconds;

        hashFunction = Hashing.murmur3_32();

        lookupDataPhaser = new Phaser(1);

        writeCache = new LinkedHashMap<Path, LookupData>(maxCacheSize + 1, 1.1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Path, LookupData> eldest) {
                if (size() > maxCacheSize) {
                    Path path = eldest.getKey();
                    log.trace("cache removing {}", path);
                    try {
                        eldest.getValue().close();
                    } catch (IOException e) {
                        log.error("unable to close " + path, e);
                        throw new UncheckedIOException("unable to close " + path, e);
                    } finally {
                        lookupDataPhaser.arriveAndDeregister();
                    }
                    return true;
                }
                return false;
            }
        };
    }

    public long get(String partition, String key) {
        validatePartition(partition);

        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        Path metaPath = lenPath.resolve("meta");
        if (!Files.exists(metaPath)) {
            log.trace("no metadata for key {} at path {}", key, metaPath);
            return -1;
        }
        LookupMetadata metadata = new LookupMetadata(lenPath.resolve("meta"));
        return metadata.readData(lenPath.resolve("data"), lookupKey);
    }

    public void put(String partition, String key, long value) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        loadFromWriteCache(lenPath).put(lookupKey, value);
    }

    private LookupData loadFromWriteCache(Path lenPath) {
        synchronized (writeCache) {
            return writeCache.computeIfAbsent(lenPath, path -> {
                log.trace("cache loading {}", lenPath);
                return new LookupData(
                        parseKeyLengthFromPath(lenPath),
                        lenPath.resolve("data"),
                        lenPath.resolve("meta"),
                        flushDelaySeconds
                );
            });
        }
    }

    public long putIfNotExists(String partition, String key, LongSupplier allocateLongFunc) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        //noinspection ConstantConditions
        return loadFromWriteCache(lenPath).putIfNotExists(lookupKey, allocateLongFunc);
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
        lookupDataPhaser.arriveAndAwaitAdvance();
        log.trace("closed {}", dir);
    }

    public void clear() {
        log.info("clearing {}", dir);
        close();
        try {
            Path tmpDir = Files.createTempFile(dir.getParent(), dir.getFileName().toString(), ".defunct");
            Files.delete(tmpDir);
            Files.move(dir, tmpDir);
            SafeDeleting.removeDirectory(tmpDir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to delete lookups: " + dir, e);
        }
    }

    private Path hashAndLengthPath(String partition, LookupKey key) {
        log.trace("getting from {}: {}", dir, key);
        byte[] hash = hashFunction.hashString(key.string(), Charsets.UTF_8).asBytes();
        String hashPath = String.format("%02x/%01x/%d", 0xff & (int) hash[0], 0xf & (int) hash[1], key.byteLength());
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
        return Character.isJavaIdentifierStart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }
}
