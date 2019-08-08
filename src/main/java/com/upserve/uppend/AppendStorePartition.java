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

    static AppendStorePartition createPartition(Path parentDir, String partition, AppendOnlyStoreBuilder builder) {

        Path partitionDir = validatePartition(parentDir, partition);

        BlockedLongs blocks = new BlockedLongs(
                blocksFile(partitionDir),
                builder.getBlobsPerBlock(),
                false,
                builder.getBlockedLongMetricsAdders()
        );
        VirtualPageFile blobs = new VirtualPageFile(
                blobsFile(partitionDir),
                builder.getLookupHashCount(),
                builder.getBlobPageSize(),
                builder.getTargetBufferSize(),
                false
        );
        VirtualPageFile metadata = new VirtualPageFile(
                metadataPath(partitionDir),
                builder.getLookupHashCount(),
                builder.getMetadataPageSize(),
                adjustedTargetBufferSize(
                        builder.getMetadataPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()),
                false
        );
        VirtualPageFile keys = new VirtualPageFile(
                keysPath(partitionDir),
                builder.getLookupHashCount(),
                builder.getLookupPageSize(),
                adjustedTargetBufferSize(
                        builder.getLookupPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()
                ),
                false
        );

        return new AppendStorePartition(keys, metadata, blobs, blocks, false, builder);
    }

    static AppendStorePartition openPartition(Path parentDir, String partition, boolean readOnly, AppendOnlyStoreBuilder builder) {
        validatePartition(partition);
        Path partitionDir = parentDir.resolve(partition);

        if (!(Files.exists(blocksFile(partitionDir)) && Files.exists(metadataPath(partitionDir))
                && Files.exists(keysPath(partitionDir)) && Files.exists(blobsFile(partitionDir)))) return null;

        BlockedLongs blocks = new BlockedLongs(
                blocksFile(partitionDir),
                builder.getBlobsPerBlock(),
                readOnly,
                builder.getBlockedLongMetricsAdders()
        );

        VirtualPageFile blobs = new VirtualPageFile(
                blobsFile(partitionDir),
                builder.getLookupHashCount(),
                builder.getBlobPageSize(),
                builder.getTargetBufferSize(),
                readOnly
        );
        VirtualPageFile metadata = new VirtualPageFile(
                metadataPath(partitionDir),
                builder.getLookupHashCount(),
                builder.getMetadataPageSize(),
                adjustedTargetBufferSize(
                        builder.getMetadataPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()
                ),
                readOnly
        );
        VirtualPageFile keys = new VirtualPageFile(
                keysPath(partitionDir),
                builder.getLookupHashCount(),
                builder.getLookupPageSize(),
                adjustedTargetBufferSize(
                        builder.getLookupPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()
                ),
                readOnly
        );

        return new AppendStorePartition(keys, metadata, blobs, blocks, readOnly, builder);
    }

    private AppendStorePartition(
            VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, VirtualPageFile blobsFile,
            BlockedLongs blocks, boolean readOnly, AppendOnlyStoreBuilder builder) {
        super(longKeyFile, metadataBlobFile, readOnly, builder);

        this.blocks = blocks;
        this.blobFile = blobsFile;

        blobs = IntStream.range(0, hashCount)
                .mapToObj(virtualFileNumber -> new VirtualAppendOnlyBlobStore(
                        virtualFileNumber, blobsFile, builder.getBlobStoreMetricsAdders()
                        )
                )
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
        return IntStream.range(0, hashCount)
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
        IntStream.range(0, hashCount)
                .parallel()
                .boxed()
                .forEach(virtualFileNumber ->
                        lookups[virtualFileNumber].scan().forEach(entry -> callback.accept(entry.getKey().string(), blocks.values(entry.getValue()).mapToObj(blobs[virtualFileNumber]::read))
                        ));
    }

    Stream<String> keys() {
        return IntStream.range(0, hashCount)
                .parallel()
                .boxed()
                .flatMap(virtualFileNumber -> lookups[virtualFileNumber].keys().map(LookupKey::string));
    }

    void clear() throws IOException {
        getLongKeyFile().close();
        getMetadataBlobFile().close();
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

    BlockedLongs getBlocks() {
        return blocks;
    }

    VirtualPageFile getBlobFile() {
        return blobFile;
    }
}
