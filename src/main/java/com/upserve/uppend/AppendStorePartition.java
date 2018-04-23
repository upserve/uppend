package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Stream;

public class AppendStorePartition extends Partition implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path partitiondDir;
    private final LongLookup lookups;
    private final BlobStore blobStore;

    private static Path blobsFile(Path partitiondDir){
        return partitiondDir.resolve("blobStore");
    }

    public static AppendStorePartition createPartition(Path partentDir, String partition, int hashSize, PageCache blobPageCache, LookupCache lookupCache){
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        return new AppendStorePartition(
                partitiondDir,
                new LongLookup(lookupsDir(partitiondDir), hashSize, PartitionLookupCache.create(partition, lookupCache)),
                new BlobStore(blobsFile(partitiondDir), blobPageCache)
                );
    }

    public static AppendStorePartition openPartition(Path partentDir, String partition, int hashSize, PageCache blobPageCache, LookupCache lookupCache) {
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        Path blobsFile = blobsFile(partitiondDir);
        Path lookupsDir = lookupsDir(partitiondDir);

        if (Files.exists(blobsFile)) {
            return new AppendStorePartition(
                    partitiondDir,
                    new LongLookup(lookupsDir, hashSize, PartitionLookupCache.create(partition, lookupCache)),
                    new BlobStore(blobsFile, blobPageCache)
            );
        } else {
            return null;
        }
    }

    private AppendStorePartition(Path partentDir, LongLookup longLookup, BlobStore blobStore){
        partitiondDir = partentDir;
        this.lookups = longLookup;
        this.blobStore = blobStore;
    }

    void append(String key, byte[] blob, BlockedLongs blocks){
        long blobPos = blobStore.append(blob);
        long blockPos = lookups.putIfNotExists(key, blocks::allocate);
        blocks.append(blockPos, blobPos);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for path '{}', key '{}'", blob.length, blobPos, blockPos, partitiondDir, key);
    }

    Stream<byte[]> read(String key, BlockedLongs blocks){
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
        return blocks.values(lookups.getLookupData(key)).parallel().mapToObj(blobStore::read);
    }

    Stream<byte[]> readSequential(String key, BlockedLongs blocks){
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
        return blocks.values(lookups.getLookupData(key)).mapToObj(blobStore::read);
    }

    byte[] readLast(String key, BlockedLongs blocks) {
        return blobStore.read(blocks.lastValue(lookups.getLookupData(key)));
    }

    Stream<Map.Entry<String, Stream<byte[]>>> scan(BlockedLongs blocks){
        return lookups.scan()
                .map(entry -> Maps.immutableEntry(
                        entry.getKey(),
                        blocks.values(entry.getValue()).mapToObj(blobStore::read)
                ));
    }

    long size() {
        return blobStore.getPosition();
    }

    void scan(BlockedLongs blocks, BiConsumer<String, Stream<byte[]>> callback){
        lookups.scan((s, value) -> callback.accept(s, blocks.values(value).mapToObj(blobStore::read)));
    }

    Stream<String> keys(){
        return lookups.keys();
    }

    @Override
    public void flush() {
        lookups.flush();
        blobStore.flush();
    }

    void clear(){
        lookups.clear();
        blobStore.clear();
    }
}
