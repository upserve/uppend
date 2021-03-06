package com.upserve.uppend;

import com.upserve.uppend.lookup.LookupData;
import com.upserve.uppend.metrics.*;
import com.upserve.uppend.metrics.LookupDataMetrics;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

public class FileAppendOnlyStore extends FileStore<AppendStorePartition> implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Function<String, AppendStorePartition> openPartitionFunction;
    private final Function<String, AppendStorePartition> createPartitionFunction;

    final BlobStoreMetrics.Adders blobStoreMetricsAdders;
    final BlockedLongMetrics.Adders blockedLongMetricsAdders;

    FileAppendOnlyStore(boolean readOnly, AppendOnlyStoreBuilder builder) {
        super(readOnly, builder);

        openPartitionFunction = partitionKey -> AppendStorePartition.openPartition(partitionsDir, partitionKey, readOnly, builder);

        createPartitionFunction = partitionKey -> AppendStorePartition.createPartition(partitionsDir, partitionKey, builder);

        blobStoreMetricsAdders = builder.getBlobStoreMetricsAdders();
        blockedLongMetricsAdders = builder.getBlockedLongMetricsAdders();
    }

    @Override
    public String getName() {
        return name;
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
    Function<String, AppendStorePartition> getOpenPartitionFunction() {
        return openPartitionFunction;
    }

    @Override
    Function<String, AppendStorePartition> getCreatePartitionFunction() {
        return createPartitionFunction;
    }

    @Override
    public BlockedLongMetrics getBlockedLongMetrics() {
        LongSummaryStatistics blockedLongAllocatedBlocksStatistics = streamPartitions()
                .mapToLong(partition -> partition.getBlocks().getBlockCount())
                .summaryStatistics();

        LongSummaryStatistics blockedLongAppendCountStatistics = streamPartitions()
                .mapToLong(partition -> partition.getBlocks().getCount())
                .summaryStatistics();

        return new BlockedLongMetrics(
                blockedLongMetricsAdders, blockedLongAllocatedBlocksStatistics, blockedLongAppendCountStatistics
        );
    }

    @Override
    public BlobStoreMetrics getBlobStoreMetrics() {
        LongSummaryStatistics blobStoreAllocatedPagesStatistics = streamPartitions()
                .mapToLong(partition -> partition.getBlobFile().getAllocatedPageCount())
                .summaryStatistics();

        return new BlobStoreMetrics(blobStoreMetricsAdders, blobStoreAllocatedPagesStatistics);
    }

    @Override
    public LookupDataMetrics getLookupDataMetrics() {
        return super.getLookupDataMetrics();
    }

    @Override
    public MutableBlobStoreMetrics getMutableBlobStoreMetrics() {
        return super.getMutableBlobStoreMetrics();
    }

    @Override
    public LongBlobStoreMetrics getLongBlobStoreMetrics() {
        return super.getLongBlobStoreMetrics();
    }
}
