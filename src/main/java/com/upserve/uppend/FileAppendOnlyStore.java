package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.blobs.PageCache;
import com.upserve.uppend.lookup.LookupCache;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

import static com.upserve.uppend.Partition.listPartitions;

public class FileAppendOnlyStore extends FileStore<AppendStorePartition> implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final PageCache blobPageCache;
    private final PageCache keyPageCache;
    private final LookupCache lookupCache;

    private final Function<String, AppendStorePartition> openPartitionFunction;
    private final Function<String, AppendStorePartition> createPartitionFunction;

    FileAppendOnlyStore(boolean readOnly, AppendOnlyStoreBuilder builder) {
        super(builder.getDir(), builder.getFlushDelaySeconds(), readOnly);

        blobPageCache = builder.buildBlobPageCache(getName());

        keyPageCache = builder.buildLookupPageCache(getName());

        lookupCache = builder.buildLookupCache(getName());

        openPartitionFunction = partitionKey -> AppendStorePartition.openPartition(partionPath(builder.getDir()), partitionKey, builder.getLookupHashSize(), builder.getFlushThreshold(), builder.getMetadataPageSize(), builder.getBlobsPerBlock(), blobPageCache, keyPageCache, lookupCache, readOnly);

        createPartitionFunction = partitionKey -> AppendStorePartition.createPartition(partionPath(builder.getDir()), partitionKey, builder.getLookupHashSize(), builder.getFlushThreshold(), builder.getMetadataPageSize(), builder.getBlobsPerBlock(), blobPageCache, keyPageCache, lookupCache);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheStats getBlobPageCacheStats() {
        return blobPageCache.stats();
    }

    @Override
    public CacheStats getKeyPageCacheStats() {
        return keyPageCache.stats();
    }

    @Override
    public CacheStats getLookupKeyCacheStats() {
        return lookupCache.keyStats();
    }

    @Override
    public CacheStats getMetadataCacheStats() {
        return lookupCache.metadataStats();
    }

    @Override
    public long keyCount() {
        return listPartitions(partionPath(dir))
                .map(this::getIfPresent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .mapToLong(AppendStorePartition::keyCount)
                .sum();
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("appending for partition '{}', key '{}'", partition, key);
        if (readOnly) throw new RuntimeException("Can not append to store opened in read only mode:" + dir);
        getOrCreate(partition).append(key, value);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        log.trace("reading in partition {} with key {}", partition, key);

        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.read(key))
                .orElse(Stream.empty());
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        log.trace("reading sequential in partition {} with key {}", partition, key);
        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.readSequential(key))
                .orElse(Stream.empty());
    }

    public byte[] readLast(String partition, String key) {
        log.trace("reading last in partition {} with key {}", partition, key);
        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.readLast(key))
                .orElse(null);
    }

    @Override
    public Stream<String> keys(String partition) {
        log.trace("getting keys in partition {}", partition);
        return getIfPresent(partition)
                .map(AppendStorePartition::keys)
                .orElse(Stream.empty());
    }

    @Override
    public Stream<String> partitions() {
        return listPartitions(partionPath(dir));
    }

    @Override
    public Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition) {
        return getIfPresent(partition)
                .map(AppendStorePartition::scan)
                .orElse(Stream.empty());
    }

    @Override
    public void scan(String partition, BiConsumer<String, Stream<byte[]>> callback) {
        getIfPresent(partition)
                .ifPresent(partitionObject -> partitionObject.scan(callback));
    }

    @Override
    public void clear() {
        if (readOnly) throw new RuntimeException("Can not clear a store opened in read only mode:" + dir);

        log.trace("clearing");

        listPartitions(partionPath(dir))
                .map(this::getIfPresent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(appendStorePartition -> {
                    try {
                        appendStorePartition.clear();
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error clearing store " + dir, e);
                    }
                });
        partitionMap.clear();
        lookupCache.flush();
        blobPageCache.flush();
        keyPageCache.flush();
    }

    @Override
    Function<String, AppendStorePartition> getOpenPartitionFunction() {
        return openPartitionFunction;
    }

    @Override
    Function<String, AppendStorePartition> getCreatePartitionFunction() {
        return createPartitionFunction;
    }

    @Override
    protected void flushInternal() {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        // Check non null because the super class is registered in the autoflusher before the constructor finishes
        if (readOnly) throw new RuntimeException("Can not flush a store opened in read only mode:" + dir);

        partitionMap.values().parallelStream().forEach(appendStorePartition -> {
            try {
                appendStorePartition.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("Error flushing store " + dir, e);
            }
        });
    }

    @Override
    public void trimInternal() {
        if (!readOnly) flushInternal();
        lookupCache.flush();
        blobPageCache.flush();
        keyPageCache.flush();
    }

    @Override
    protected void closeInternal() {
        partitionMap.values().parallelStream().forEach(appendStorePartition -> {
            try {
                appendStorePartition.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Error closing store " + dir, e);
            }
        });

        lookupCache.flush();
        blobPageCache.flush();
        keyPageCache.flush();
    }
}
