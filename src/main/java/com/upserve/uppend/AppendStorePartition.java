package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import com.upserve.uppend.util.SafeDeleting;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Math.min;

public class AppendStorePartition extends Partition implements Flushable, Closeable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockedLongs blocks;
    private final VirtualAppendOnlyBlobStore[] blobs;
    private final VirtualPageFile blobFile;

    private static Path blobsFile(Path partitiondDir) {
        return partitiondDir.resolve("blobStore");
    }

    private static Path blocksFile(Path partitiondDir) {
        return partitiondDir.resolve("blockedLongs");
    }

    public static AppendStorePartition createPartition(Path parentDir, String partition, int hashSize, int targetBufferSize, int flushThreshold, int reloadInterval, int metadataPageSize, int blockSize, int blobPageSize, int keyPageSize) {
        validatePartition(partition);
        Path partitionDir = parentDir.resolve(partition);
        try {
            Files.createDirectories(partitionDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to make partition directory: " + partitionDir, e);
        }

        BlockedLongs blocks = new BlockedLongs(blocksFile(partitionDir), blockSize, false);

        VirtualPageFile blobs = new VirtualPageFile(blobsFile(partitionDir), hashSize, blobPageSize, targetBufferSize,false);
        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitionDir), hashSize, metadataPageSize, adjustedTargetBufferSize(metadataPageSize, hashSize, targetBufferSize),false);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitionDir), hashSize, keyPageSize, adjustedTargetBufferSize(keyPageSize, hashSize, targetBufferSize),false);


        return new AppendStorePartition(keys, metadata, blobs, blocks, hashSize, flushThreshold, reloadInterval, false);
    }

    public static AppendStorePartition openPartition(Path parentDir, String partition, int hashSize, int targetBufferSize, int flushThreshold, int reloadInterval, int metadataPageSize, int blockSize, int blobPageSize, int keyPageSize, boolean readOnly) {
        validatePartition(partition);
        Path partitionDir = parentDir.resolve(partition);

        if (!(Files.exists(blocksFile(partitionDir)) && Files.exists(metadataPath(partitionDir))
                && Files.exists(keysPath(partitionDir)) && Files.exists(blobsFile(partitionDir)))) return null;

        BlockedLongs blocks = new BlockedLongs(blocksFile(partitionDir), blockSize, readOnly);

        VirtualPageFile blobs = new VirtualPageFile(blobsFile(partitionDir), hashSize, blobPageSize, targetBufferSize, readOnly);
        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitionDir), hashSize, metadataPageSize, adjustedTargetBufferSize(metadataPageSize, hashSize, targetBufferSize), readOnly);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitionDir), hashSize, keyPageSize, adjustedTargetBufferSize(keyPageSize, hashSize, targetBufferSize), readOnly);

        return new AppendStorePartition(keys, metadata, blobs, blocks, hashSize, flushThreshold, reloadInterval, readOnly);
    }

    PartitionStats getPartitionStats(){
        LongSummaryStatistics metadataStats = Arrays.stream(lookups)
                .mapToLong(LookupData::getMetadataSize)
                //.filter(val -> val > 0)
                .summaryStatistics();

        return new PartitionStats(metadataBlobFile.getAllocatedPageCount(),
                longKeyFile.getAllocatedPageCount(),
                blobFile.getAllocatedPageCount(),
                Arrays.stream(lookups).mapToLong(LookupData::getMetadataLookupMissCount).sum(),
                Arrays.stream(lookups).mapToLong(LookupData::getMetadataLookupHitCount).sum(),
                metadataStats.getSum(),
                Arrays.stream(lookups).mapToLong(LookupData::getFindKeyTimer).sum(),
                Arrays.stream(lookups).mapToLong(LookupData::getFlushedKeyCount).sum(),
                Arrays.stream(lookups).mapToLong(LookupData::getFlushCount).sum(),
                metadataStats.getCount(),
                metadataStats.getMax()
                );
    }

    private AppendStorePartition(VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, VirtualPageFile blobsFile, BlockedLongs blocks, int hashSize, int flushThreshold, int reloadInterval, boolean readOnly) {
        super(longKeyFile, metadataBlobFile, hashSize, flushThreshold, reloadInterval, readOnly);

        this.blocks = blocks;
        this.blobFile = blobsFile;
        blobs = IntStream.range(0, hashSize)
                .mapToObj(virtualFileNumber -> new VirtualAppendOnlyBlobStore(virtualFileNumber, blobsFile))
                .toArray(VirtualAppendOnlyBlobStore[]::new);
    }

    void append(String key, byte[] blob) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        final long blobPos = blobs[hash].append(blob);
        final long blockPos = lookups[hash].putIfNotExists(lookupKey, blocks::allocate);
        blocks.append(blockPos, blobPos);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for hash '{}', key '{}'", blob.length, blobPos, blockPos, hash, key);
    }

    Stream<byte[]> read(String key) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        // Values stream can now be parallel, but it breaks everything...
        LongStream longStream = blocks.values(lookups[hash].getValue(lookupKey));
        return longStream.mapToObj(blobs[hash]::read);

        //return blocks.lazyValues(lookups[hash].getValue(lookupKey)).parallel().mapToObj(blobs[hash]::read);
    }

    Stream<byte[]> readSequential(String key) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        return blocks.values(lookups[hash].getValue(lookupKey)).mapToObj(blobs[hash]::read);
    }

    byte[] readLast(String key) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        return blobs[hash].read(blocks.lastValue(lookups[hash].getValue(lookupKey)));
    }

    Stream<Map.Entry<String, Stream<byte[]>>> scan() {
        return IntStream.range(0, hashSize)
                .parallel()
                .boxed()
                .flatMap(virtualFileNumber ->
                        lookups[virtualFileNumber].scan().map(entry -> Maps.immutableEntry(
                                entry.getKey().string(),
                                blocks.values(entry.getValue()).mapToObj(blobs[virtualFileNumber]::read)
                        ))
                );
    }

    void scan(BiConsumer<String, Stream<byte[]>> callback) {
        IntStream.range(0, hashSize)
                .parallel()
                .boxed()
                .forEach(virtualFileNumber ->
                        lookups[virtualFileNumber].scan().forEach(entry -> callback.accept(entry.getKey().string(), blocks.values(entry.getValue()).mapToObj(blobs[virtualFileNumber]::read))
                        ));
    }

    Stream<String> keys() {
        return IntStream.range(0, hashSize)
                .parallel()
                .boxed()
                .flatMap(virtualFileNumber -> lookups[virtualFileNumber].keys().map(LookupKey::string));
    }

    BlockStats blockedLongStats() {
        return blocks.stats();
    }

    void clear() throws IOException {
        longKeyFile.close();
        metadataBlobFile.close();
        blobFile.close();
        blocks.close();

        SafeDeleting.removeDirectory(longKeyFile.getFilePath().getParent());
    }

    @Override
    public void close() throws IOException {
        super.close();

        blobFile.close();
        blocks.close();
    }
}
