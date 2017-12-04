package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.stream.*;

public class FileAppendOnlyStore extends FileStore implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int NUM_BLOBS_PER_BLOCK = 127;

    private final LongLookup lookups;
    private final BlockedLongs blocks;
    private final Blobs blobs;

    FileAppendOnlyStore(Path dir, int flushDelaySeconds, boolean doLock, int longLookupHashSize, int longLookupWriteCacheSize) {
        super(dir, flushDelaySeconds, doLock);

        lookups = new LongLookup(
                dir.resolve("lookups"),
                longLookupHashSize,
                longLookupWriteCacheSize
        );
        blocks = new BlockedLongs(dir.resolve("blocks"), NUM_BLOBS_PER_BLOCK);
        blobs = new Blobs(dir.resolve("blobs"));
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
    protected void flushInternal() {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        lookups.flush();
        blocks.flush();
        blobs.flush();
    }

    @Override
    protected void closeInternal() {
        try {
            lookups.close();
        } catch (Exception e) {
            log.error("unable to close lookups", e);
        }
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
