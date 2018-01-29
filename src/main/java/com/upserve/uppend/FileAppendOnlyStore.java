package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.upserve.uppend.lookup.LongLookup;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.*;

public class FileAppendOnlyStore extends FileStore implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final int NUM_BLOBS_PER_BLOCK = 127;

    protected final LongLookup lookups;
    protected final BlockedLongs blocks;
    protected final Blobs blobs;

    protected FileAppendOnlyStore(Path dir, int flushDelaySeconds, boolean doLock, int longLookupHashSize, int longLookupWriteCacheSize, int blobsPerBlock) {
        super(dir, flushDelaySeconds, doLock);

        lookups = new LongLookup(
                dir.resolve("lookups"),
                longLookupHashSize,
                longLookupWriteCacheSize
        );
        blocks = new BlockedLongs(dir.resolve("blocks"), blobsPerBlock);
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
    public void purgeWriteCache() {
        lookups.close();
        blocks.flush();
        blobs.flush();
    }

    @Override
    public AppendStoreStats cacheStats(){
        return new AppendStoreStats(blobs.size(), blocks.size(), lookups.cacheSize(), lookups.cacheEntries(), lookups.writeCacheTasks(), 0, 0, 0);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        log.trace("reading in partition {} with key {}", partition, key);
        return blockValues(partition, key, true)
                .parallel()
                .mapToObj(blobs::read);
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        log.trace("reading sequential in partition {} with key {}", partition, key);
        return blockValues(partition, key, true)
                .mapToObj(blobs::read);
    }

    public byte[] readLast(String partition, String key) {
        log.trace("reading last in partition {} with key {}", partition, key);
        long pos = blockLastValue(partition, key, true);
        if (pos == -1) {
            return null;
        }
        return blobs.read(pos);
    }

    public Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition){
        return lookups.scan(partition)
                .map(entry ->
                        Maps.immutableEntry(
                                entry.getKey(),
                                blocks.values(entry.getValue())
                                        .parallel()
                                        .mapToObj(blobs::read)
                        )
                );
    }

    @Override
    public long size() {
        return partitions().parallel().flatMapToLong(lookups::size).sum();
    }

    @Override
    public Stream<byte[]> readFlushed(String partition, String key) {
        log.trace("reading cached in partition {} with key {}", partition, key);
        return blockValues(partition, key, false)
                .parallel()
                .mapToObj(blobs::read);
    }

    @Override
    public Stream<byte[]> readSequentialFlushed(String partition, String key) {
        log.trace("reading sequential cached in partition {} with key {}", partition, key);
        return blockValues(partition, key, false)
                .mapToObj(blobs::read);
    }

    @Override
    public byte[] readLastFlushed(String partition, String key) {
        log.trace("reading last cached in partition {} with key {}", partition, key);
        long pos = blockLastValue(partition, key, false);
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
        // Check non null because the super class is registered in the autoflusher before the constructor finishes
        if (lookups != null) lookups.flush();
        if (blobs != null) blocks.flush();
        if (blobs != null) blobs.flush();
    }

    @Override
    protected void closeInternal() {
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
        try {
            blocks.close();
        } catch (Exception e) {
            log.error("unable to close blocks", e);
        }

    }

    private LongStream blockValues(String partition, String key, boolean useCache) {
        log.trace("reading block values for key: {}", key);
        long blockPos = blockPos(partition, key, useCache);
        if (blockPos == -1) {
            log.trace("key not found: {}", key);
            return LongStream.empty();
        }
        log.trace("streaming values at block pos {} for key: {}", blockPos, key);
        return blocks.values(blockPos);
    }

    private long blockLastValue(String partition, String key, boolean useCache) {
        log.trace("reading last value for key: {}", key);
        long blockPos = blockPos(partition, key, useCache);
        if (blockPos == -1) {
            log.trace("key not found: {}", key);
            return -1;
        }
        log.trace("returning last value at block pos {} for key: {}", blockPos, key);
        return blocks.lastValue(blockPos);
    }

    private long blockPos(String partition, String key, boolean useCache) {
        if (useCache) {
            return lookups.get(partition, key);
        } else {
            return lookups.getFlushed(partition, key);
        }
    }
}
