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
import java.util.function.BiConsumer;
import java.util.stream.*;

public class AppendStorePartition extends Partition implements Flushable, Closeable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int MAX_HASH_SIZE = 1 << 24; /* 16,777,216 */


    private final BlockedLongs blocks;
    private final VirtualAppendOnlyBlobStore[] blobs;
    private final VirtualPageFile blobFile;

    private static Path blobsFile(Path partitiondDir) {
        return partitiondDir.resolve("blobStore");
    }

    private static Path blocksFile(Path partitiondDir) {
        return partitiondDir.resolve("blockedLongs");
    }

    public static AppendStorePartition createPartition(Path parentDir, String partition, int hashSize, int flushThreshold, int metadataPageSize, int blockSize, PageCache blobPageCache, PageCache keyPageCache, LookupCache lookupCache) {
        validatePartition(partition);
        Path partitiondDir = parentDir.resolve(partition);
        try {
            Files.createDirectories(partitiondDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to make partition directory: " + partitiondDir, e);
        }

        BlockedLongs blocks = new BlockedLongs(blocksFile(partitiondDir), blockSize, false);

        VirtualPageFile blobs = new VirtualPageFile(blobsFile(partitiondDir), hashSize, false, blobPageCache);
        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitiondDir), hashSize, metadataPageSize, false);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitiondDir), hashSize, false, keyPageCache);


        return new AppendStorePartition(keys, metadata, blobs, blocks, PartitionLookupCache.create(partition, lookupCache), hashSize, flushThreshold, false);
    }

    public static AppendStorePartition openPartition(Path parentDir, String partition, int hashSize, int flushThreshold, int metadataPageSize, int blockSize, PageCache blobPageCache, PageCache keyPageCache, LookupCache lookupCache, boolean readOnly) {
        validatePartition(partition);
        Path partitiondDir = parentDir.resolve(partition);

        if (!(Files.exists(blocksFile(partitiondDir)) && Files.exists(metadataPath(partitiondDir))
                && Files.exists(keysPath(partitiondDir)) && Files.exists(blobsFile(partitiondDir)))) return null;

        BlockedLongs blocks = new BlockedLongs(blocksFile(partitiondDir), blockSize, readOnly);

        VirtualPageFile blobs = new VirtualPageFile(blobsFile(partitiondDir), hashSize, readOnly, blobPageCache);
        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitiondDir), hashSize, metadataPageSize, readOnly);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitiondDir), hashSize, readOnly, keyPageCache);

        return new AppendStorePartition(keys, metadata, blobs, blocks, PartitionLookupCache.create(partition, lookupCache), hashSize, flushThreshold, false);
    }

    private AppendStorePartition(VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, VirtualPageFile blobsFile, BlockedLongs blocks, PartitionLookupCache lookupCache, int hashSize, int flushThreshold, boolean readOnly) {
        super(longKeyFile, metadataBlobFile, lookupCache, hashSize, flushThreshold, readOnly);


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

        return blocks.values(lookups[hash].getValue(lookupKey)).parallel().mapToObj(blobs[hash]::read);
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
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

    @Override
    public void flush() throws IOException {
        log.debug("Starting flush for partition: {}", lookupCache.getPartition());

        Arrays.stream(lookups).parallel().forEach(LookupData::flush);

        longKeyFile.flush();
        metadataBlobFile.flush();
        blobFile.flush();
        blocks.flush();

        log.debug("Finished flush for partition: {}", lookupCache.getPartition());
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
        flush();

        longKeyFile.close();
        metadataBlobFile.close();
        blobFile.close();
        blocks.close();
    }
}
