package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.upserve.uppend.lookup.LongLookup;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.*;

public class FileAppendOnlyStore extends FileStore implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final int NUM_BLOBS_PER_BLOCK = 127;

    protected final LongLookup lookups;
    protected final BlockedLongs blocks;

    protected FileAppendOnlyStore(Path dir, int flushDelaySeconds, boolean doLock, int longLookupHashSize, int longLookupWriteCacheSize, int blobsPerBlock) {
        super(dir, flushDelaySeconds, doLock);

        boolean readOnly = !doLock;

        blocks = new BlockedLongs(dir.resolve("blocks"), blobsPerBlock, readOnly);

        lookups = new LongLookup(
                dir.resolve("lookups"),
                blocks,
                longLookupHashSize,
                longLookupWriteCacheSize
        );

    }

    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("appending for key '{}'", key);
        long blockPos = lookups.putIfNotExists(partition, key, value);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for key '{}'", value.length, blockPos, key);
    }

    @Override
    public void purgeWriteCache() {
        lookups.close();
        blocks.flush();
    }

    @Override
    public AppendStoreStats cacheStats(){
        return new AppendStoreStats(0, blocks.size(), lookups.cacheSize(), lookups.cacheEntries(), lookups.writeCacheTasks(), 0, 0, 0);
    }

    @Override
    public String blockStats() {
        return blocks.stats();
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        log.trace("reading in partition {} with key {}", partition, key);
        return lookups.read(partition, key);
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        log.trace("reading sequential in partition {} with key {}", partition, key);
//        return blockValues(partition, key, true)
//                .mapToObj(blobs::read);
        return Stream.empty();
    }

    public byte[] readLast(String partition, String key) {
        log.trace("reading last in partition {} with key {}", partition, key);
//        long pos = blockLastValue(partition, key, true);
//        if (pos == -1) {
//            return null;
//        }
        return new byte[]{};
    }

    public Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition){
//        return lookups.scan(partition)
//                .map(entry ->
//                        Maps.immutableEntry(
//                                entry.getKey(),
//                                blocks.values(entry.getValue())
//                                        .parallel()
//                                        .mapToObj(blobs::read)
//                        )
//                );
        return Stream.empty();
    }

    @Override
    public long size() {
        return partitions().parallel().flatMapToLong(lookups::size).sum();
    }

    @Override
    public Stream<byte[]> readFlushed(String partition, String key) {
        log.trace("reading cached in partition {} with key {}", partition, key);
        return Stream.empty();
    }

    @Override
    public Stream<byte[]> readSequentialFlushed(String partition, String key) {
        log.trace("reading sequential cached in partition {} with key {}", partition, key);
        return Stream.empty();
    }

    @Override
    public byte[] readLastFlushed(String partition, String key) {
        log.trace("reading last cached in partition {} with key {}", partition, key);
//        long pos = blockLastValue(partition, key, false);
//        if (pos == -1) {
//            return null;
//        }
        return new byte[]{};
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
        lookups.clear();
    }

    @Override
    protected void flushInternal() {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        // Check non null because the super class is registered in the autoflusher before the constructor finishes
        if (lookups != null) lookups.flush();
        if (blocks != null) blocks.flush();
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

    }
}
