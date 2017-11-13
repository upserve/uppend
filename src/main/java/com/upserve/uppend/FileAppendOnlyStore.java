package com.upserve.uppend;

import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.*;

public class FileAppendOnlyStore implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

    private final AtomicBoolean isClosed;

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

        isClosed = new AtomicBoolean(false);
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("appending for key '{}'", key);
        long blobPos = blobs.append(value);
        long blockPos = lookups.putIfNotExists(partition, key, blocks::allocate);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for key '{}'", value.length, blobPos, blockPos, key);
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
        log.trace("reading in partition {} with key {}", partition, key);
        return blockValues(partition, key)
                .parallel()
                .mapToObj(blobs::read);
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        log.trace("reading sequential in partition {} with key {}", partition, key);
        return blockValues(partition, key)
                .mapToObj(blobs::read);
    }

    public byte[] readLast(String partition, String key) {
        log.trace("reading last in partition {} with key {}", partition, key);
        long pos = blockLastValue(partition, key);
        if (pos == -1) {
            return null;
        }
        return blobs.read(pos);
    }

    @Override
    public Stream<String> keys(String partition) {
        log.trace("getting keys in partition {}", partition);
        return lookups.keys(partition);
    }

    @Override
    public Stream<String> partitions() {
        log.trace("getting partitions");
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
        if (!isClosed.compareAndSet(false, true)) {
            log.warn("close called twice on store: " + dir);
            return;
        }
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
        log.trace("reading block values for key: {}", key);
        long blockPos = lookups.get(partition, key);
        if (blockPos == -1) {
            log.trace("key not found: {}", key);
            return LongStream.empty();
        }
        log.trace("streaming values at block pos {} for key: {}", blockPos, key);
        return blocks.values(blockPos);
    }

    private long blockLastValue(String partition, String key) {
        log.trace("reading last valye for key: {}", key);
        long blockPos = lookups.get(partition, key);
        if (blockPos == -1) {
            log.trace("key not found: {}", key);
            return -1;
        }
        log.trace("streaming values at block pos {} for key: {}", blockPos, key);
        return blocks.lastValue(blockPos);
    }
}
