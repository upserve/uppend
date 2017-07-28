package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.*;
import com.google.common.base.Charsets;
import com.google.common.hash.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.Comparator;
import java.util.function.LongSupplier;
import java.util.stream.*;

@Slf4j
public class LongLookup implements AutoCloseable {
    private static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    private static final int DEFAULT_MAX_CACHE_SIZE = 1024;

    private final Path dir;
    private final HashFunction hashFunction;
    private final LoadingCache<Path, LookupData> cache;

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

        hashFunction = Hashing.murmur3_32();

        cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .removalListener((RemovalListener<Path, LookupData>) (key, value, cause) -> {
                    try {
                        if (value != null) {
                            value.close();
                        }
                    } catch (IOException e) {
                        log.error("unable to close " + key, e);
                        throw new UncheckedIOException("unable to close " + key, e);
                    }
                })
                .build((lenPath) -> new LookupData(
                        parseKeyLengthFromPath(lenPath),
                        lenPath.resolve("data"),
                        lenPath.resolve("meta"),
                        flushDelaySeconds)
                );
    }

    public long get(String partition, String key) {
        validatePartition(partition);

        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        LookupMetadata metadata = new LookupMetadata(lenPath.resolve("meta"));
        return metadata.readData(lenPath.resolve("data"), lookupKey);
    }

    public void put(String partition, String key, long value) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        cache.get(lenPath).put(lookupKey, value);
    }

    public long putIfNotExists(String partition, String key, LongSupplier allocateLongFunc) {
        validatePartition(partition);
        LookupKey lookupKey = new LookupKey(key);
        Path lenPath = hashAndLengthPath(partition, lookupKey);
        return cache.get(lenPath).putIfNotExists(lookupKey, allocateLongFunc);
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
                .map(p -> p.getFileName().toString());
    }

    @Override
    public void close() {
        log.trace("closing {}", dir);
        cache.invalidateAll();
        cache.cleanUp();
    }

    public void clear() {
        log.info("clearing {}", dir);
        close();
        try {
            Path tmpDir = Files.createTempFile(dir.getParent(), dir.getFileName().toString(), ".defunct");
            Files.delete(tmpDir);
            Files.move(dir, tmpDir);
            deleteDirectory(tmpDir);
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

    private static void deleteDirectory(Path path) throws IOException {
        if (path == null || path.toFile().getAbsolutePath().length() < 4) {
            throw new IOException("refusing to delete null or short path: " + path);
        }
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
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
