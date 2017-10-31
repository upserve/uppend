package com.upserve.uppend;

import com.upserve.uppend.lookup.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.stream.*;

@Slf4j
public class FileAppendOnlyStore implements AppendOnlyStore, Flushable {
    /**
     * DEFAULT_FLUSH_DELAY_SECONDS is the number of seconds to wait between
     * automatically flushing writes.
     */
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;

    private static final int NUM_BLOBS_PER_BLOCK = 127;

    private final Path dir;
    private final LongLookup lookups;
    private final BlockedLongs blocks;
    private final Blobs blobs;

    public FileAppendOnlyStore(Path dir) {
        this(
                dir,
                LongLookup.DEFAULT_HASH_SIZE,
                LongLookup.DEFAULT_WRITE_CACHE_SIZE,
                DEFAULT_FLUSH_DELAY_SECONDS
        );
    }

    public FileAppendOnlyStore(Path dir, int longLookupHashSize, int longLookupWriteCacheSize, int flushDelaySeconds) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        this.dir = dir;
        lookups = new LongLookup(
                dir.resolve("lookups"),
                longLookupHashSize,
                longLookupWriteCacheSize
        );
        blocks = new BlockedLongs(dir.resolve("blocks"), NUM_BLOBS_PER_BLOCK);
        blobs = new Blobs(dir.resolve("blobs"));
        AutoFlusher.register(flushDelaySeconds, this);
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        long blobPos = blobs.append(value);
        long blockPos = lookups.putIfNotExists(partition, key, blocks::allocate);
        log.trace("appending {} bytes (blob pos {}) for key '{}' at block pos {}", value.length, blobPos, key, blockPos);
        blocks.append(blockPos, blobPos);
    }

    @Override
    public void flush() {
        log.info("flushing {}", dir);
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        lookups.flush();
        blocks.flush();
        blobs.flush();
        log.info("flushed {}", dir);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        return blockValues(partition, key)
                .parallel()
                .mapToObj(blobs::read);
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        return blockValues(partition, key)
                .mapToObj(blobs::read);
    }

    @Override
    public Stream<String> keys(String partition) {
        return lookups.keys(partition);
    }

    @Override
    public Stream<String> partitions() {
        return lookups.partitions();
    }

    @Override
    public void clear() {
        log.trace("clearing");
        blocks.clear();
        blobs.clear();
        lookups.clear();
    }

    @Override
    public void close() throws Exception {
        log.info("closing: " + dir);
        AutoFlusher.deregister(this);
        try {
            blocks.close();
        } catch (Exception e) {
            log.error("unable to close blocks", e);
        }
        try {
            blobs.close();
        } catch (Exception e) {
            log.error("unable to close blobs", e);
        }
        try {
            lookups.close();
        } catch (Exception e) {
            log.error("unable to close lookups", e);
        }
    }

    private LongStream blockValues(String partition, String key) {
        log.trace("reading key: {}", key);
        long blockPos = lookups.get(partition, key);
        if (blockPos == -1) {
            log.trace("key not found: {}", key);
            return LongStream.empty();
        }
        log.trace("streaming values at block pos {} for key: {}", blockPos, key);
        return blocks.values(blockPos);
    }
}
