package com.upserve.uppend;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.*;

public class FileAppendOnlyStore extends FileStore implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected static final int DEFAULT_BLOBS_PER_BLOCK = 127;
    protected static final int DEFAULT_BLOB_PAGE_SIZE = 1024 * 1024;
    protected static final int DEFAULT_MAXIMUM_BLOB_CACHE_SIZE = 10_000;
    protected static final int DEFAULT_INITIAL_BLOB_CACHE_SIZE = 100;

    protected static final int DEFAULT_LOOKUP_PAGE_SIZE = 256 *1024;
    protected static final int DEFAULT_MAXIMUM_LOOKUP_CACHE_SIZE = 10_000;
    protected static final int DEFAULT_INITIAL_LOOKUP_CACHE_SIZE = 100;

    protected static final int DEFAULT_MAXIMUM_FILE_CACHE_SIZE = 10_000;
    protected static final int DEFAULT_INITIAL_FILE_CACHE_SIZE = 1000;

    protected static final int DEFAULT_HASH_SIZE = 256;

    protected final BlockedLongs blocks;

    private final Map<String, Partition> partitionMap;
    private final PagedFileMapper blobPageCache;
    private final PagedFileMapper lookupPageCache;
    private final LookupCache lookupCache;
    private final FileCache fileCache;

    FileAppendOnlyStore(Path dir, int flushDelaySeconds, boolean doLock, int longLookupHashSize, int longLookupWriteCacheSize, int blobsPerBlock) {
        super(dir, flushDelaySeconds, doLock);

        partitionMap = new ConcurrentHashMap<>();

        fileCache = new FileCache(DEFAULT_INITIAL_FILE_CACHE_SIZE, DEFAULT_MAXIMUM_FILE_CACHE_SIZE, false);
        blobPageCache = new PagedFileMapper(DEFAULT_BLOB_PAGE_SIZE, DEFAULT_INITIAL_BLOB_CACHE_SIZE, DEFAULT_MAXIMUM_BLOB_CACHE_SIZE, fileCache);
        lookupPageCache = new PagedFileMapper(DEFAULT_LOOKUP_PAGE_SIZE, DEFAULT_INITIAL_LOOKUP_CACHE_SIZE, DEFAULT_MAXIMUM_LOOKUP_CACHE_SIZE, fileCache);
        lookupCache = new LookupCache(lookupPageCache);

        blocks = new BlockedLongs(dir.resolve("blocks"), blobsPerBlock);
    }

    private Partition createPartition(String partition){
        return Partition.createPartition(dir.resolve("partitions"), partition, DEFAULT_HASH_SIZE, blobPageCache, lookupCache);
    }


    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("appending for partition '{}', key '{}'", partition, key);

        partitionMap.computeIfAbsent(partition, partitionKey -> Partition.createPartition(dir.resolve("partitions"), partitionKey, DEFAULT_HASH_SIZE, blobPageCache, lookupCache));
        // Not necessary to append inside the compute block
        partitionMap.get(partition).append(key, value, blocks);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        log.trace("reading in partition {} with key {}", partition, key);

        return Optional.ofNullable(partitionMap.get(partition))
                .map(partitionObject -> partitionObject.read(key, blocks))
                .orElse(Stream.empty());
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
        long pos = blockLastValue(partition, key, true);
        if (pos == -1) {
            return null;
        }
//        return blobs.read(pos);
        return new byte[]{};
    }

    @Override
    public Stream<byte[]> readFlushed(String partition, String key) {
        log.trace("reading cached in partition {} with key {}", partition, key);
//        return blockValues(partition, key, false)
//                .parallel()
//                .mapToObj(blobs::read);
        return Stream.empty();

    }

    @Override
    public Stream<byte[]> readSequentialFlushed(String partition, String key) {
        log.trace("reading sequential cached in partition {} with key {}", partition, key);
//        return blockValues(partition, key, false)
//                .mapToObj(blobs::read);
        return Stream.empty();

    }

    @Override
    public byte[] readLastFlushed(String partition, String key) {
        log.trace("reading last cached in partition {} with key {}", partition, key);
        long pos = blockLastValue(partition, key, false);
        if (pos == -1) {
            return null;
        }
//        return blobs.read(pos);
        return new byte[]{};
    }

    @Override
    public Stream<String> keys(String partition) {
        log.trace("getting keys in partition {}", partition);
//        return lookups.keys(partition);
        return Stream.empty();
    }

    @Override
    public Stream<String> partitions() {
        return partitionMap.keySet().stream();
    }

    @Override
    public Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition) {
        return Stream.empty();
//        return lookups.scan(partition)
//                .map(entry ->
//                        Maps.immutableEntry(
//                                entry.getKey(),
//                                blocks.values(entry.getValue())
//                                        .parallel()
//                                        .mapToObj(blobs::read)
//                        )
//                );
    }

    @Override
    public void scan(String partition, BiConsumer<String, Stream<byte[]>> callback) {
//        lookups.scan(
//                partition,
//                (key, blobRefs) ->  callback.accept(
//                        key,
//                        blocks.values(blobRefs).mapToObj(blobs::read)
//                )
//        );
    }

    @Override
    public void clear() {
        log.trace("clearing");
        blocks.clear();
//        blobs.clear();
//        lookups.clear();
    }

    @Override
    protected void flushInternal() {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        // Check non null because the super class is registered in the autoflusher before the constructor finishes
//        if (lookups != null) lookups.flush();
//        if (blocks != null) blocks.flush();
//        if (blobs != null) blobs.flush();
    }

    @Override
    public void trimInternal() {
//        lookups.close();
//        blocks.trim();
//        blobs.flush();
    }

    @Override
    protected void closeInternal() {
        // Close blobs first to stop appends
//        try {
//            blobs.close();
//        } catch (Exception e) {
//            log.error("unable to close blobs", e);
//        }
//        try {
//            lookups.close();
//        } catch (Exception e) {
//            log.error("unable to close lookups", e);
//        }
//        try {
//            blocks.close();
//        } catch (Exception e) {
//            log.error("unable to close blocks", e);
//        }
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
//        if (useCache) {
//            return lookups.get(partition, key);
//        } else {
//            return lookups.getFlushed(partition, key);
//        }
        return 0L;
    }
}
