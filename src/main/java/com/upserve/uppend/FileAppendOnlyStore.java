package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.lookup.*;
import com.upserve.uppend.util.SafeDeleting;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

import static com.upserve.uppend.BlockStats.ZERO_STATS;

public class FileAppendOnlyStore extends FileStore<AppendStorePartition> implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LookupCache lookupCache;

    private final Function<String, AppendStorePartition> openPartitionFunction;
    private final Function<String, AppendStorePartition> createPartitionFunction;

    FileAppendOnlyStore(boolean readOnly, AppendOnlyStoreBuilder builder) {
        super(builder.getDir(), builder.getFlushDelaySeconds(), builder.getPartitionSize(), readOnly, builder.getStoreName());

        lookupCache = builder.buildLookupCache(getName(), readOnly);

        openPartitionFunction = partitionKey -> AppendStorePartition.openPartition(partitionsDir, partitionKey, builder.getLookupHashSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getBlobsPerBlock(), builder.getBlobPageSize(), builder.getLookupPageSize(), lookupCache, readOnly);

        createPartitionFunction = partitionKey -> AppendStorePartition.createPartition(partitionsDir, partitionKey, builder.getLookupHashSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getBlobsPerBlock(), builder.getBlobPageSize(), builder.getLookupPageSize(), lookupCache);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FlushStats getFlushStats() {
        return lookupCache.getFlushStats();
    }

    @Override
    public CacheStats getLookupKeyCacheStats() {
        return lookupCache.keyStats();
    }

    @Override
    public BlockStats getBlockLongStats() {
        return partitionMap.values().parallelStream().map(AppendStorePartition::blockedLongStats).reduce(ZERO_STATS, BlockStats::add);
    }

    @Override
    public long keyCount() {
        return streamPartitions()
                .mapToLong(AppendStorePartition::keyCount)
                .sum();
    }

    @Override
    public void append(String partitionEntropy, String key, byte[] value) {
        log.trace("appending for partition '{}', key '{}'", partitionEntropy, key);
        if (readOnly) throw new RuntimeException("Can not append to store opened in read only mode:" + dir);
        getOrCreate(partitionEntropy).append(key, value);
    }

    @Override
    public Stream<byte[]> read(String partitionEntropy, String key) {
        log.trace("reading in partition {} with key {}", partitionEntropy, key);

        return getIfPresent(partitionEntropy)
                .map(partitionObject -> partitionObject.read(key))
                .orElse(Stream.empty());
    }

    @Override
    public Stream<byte[]> readSequential(String partitionEntropy, String key) {
        log.trace("reading sequential in partition {} with key {}", partitionEntropy, key);
        return getIfPresent(partitionEntropy)
                .map(partitionObject -> partitionObject.readSequential(key))
                .orElse(Stream.empty());
    }

    public byte[] readLast(String partitionEntropy, String key) {
        log.trace("reading last in partition {} with key {}", partitionEntropy, key);
        return getIfPresent(partitionEntropy)
                .map(partitionObject -> partitionObject.readLast(key))
                .orElse(null);
    }

    @Override
    public Stream<String> keys() {
        log.trace("getting keys for {}", getName());
        return streamPartitions()
                .flatMap(AppendStorePartition::keys);
    }

    @Override
    public Stream<Map.Entry<String, Stream<byte[]>>> scan() {
        return streamPartitions()
                .flatMap(AppendStorePartition::scan);
    }

    @Override
    public void scan(BiConsumer<String, Stream<byte[]>> callback) {
        streamPartitions()
                .forEach(partitionObject -> partitionObject.scan(callback));
    }

    @Override
    public void clear() {
        if (readOnly) throw new RuntimeException("Can not clear a store opened in read only mode:" + name);
        log.trace("clearing");

        closeInternal();

        try {
            SafeDeleting.removeDirectory(partitionsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clear partitions directory", e);
        }
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
        if (readOnly) throw new RuntimeException("Can not flush a store opened in read only mode:" + name);

        log.info("Flushing!");

        partitionMap.values().parallelStream().forEach(appendStorePartition -> {
            try {
                appendStorePartition.flush();
            } catch (ClosedChannelException e) {
                if (isClosed.get()) {
                    log.debug("Tried to flush a closed store {}", name, e);
                } else {
                    throw new UncheckedIOException("Error flushing store " + name, e);
                }

            } catch (IOException e) {
                if (isClosed.get())
                    throw new UncheckedIOException("Error flushing store " + name, e);
            }
        });
        log.info("Flushed!");
    }

    @Override
    public void trimInternal() {
        if (!readOnly) flushInternal();
        lookupCache.flush();
    }

    @Override
    protected void closeInternal() {
        partitionMap.values().parallelStream().forEach(appendStorePartition -> {
            try {
                appendStorePartition.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Error closing store " + name, e);
            }
        });

        partitionMap.clear();
        lookupCache.flush();
    }
}
