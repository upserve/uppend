package com.upserve.uppend;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static com.upserve.uppend.Partition.listPartitions;

public class FileAppendOnlyStore extends FileStore<AppendStorePartition> implements AppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockedLongs blocks;

    private final PageCache blobPageCache;
    private final LookupCache lookupCache;
    private final FileCache fileCache;

    private final Function<String, AppendStorePartition> openPartitionFunction;
    private final Function<String, AppendStorePartition> createPartitionFunction;

    FileAppendOnlyStore(boolean readOnly, AppendOnlyStoreBuilder builder) {
        super(builder.getDir(), builder.getFlushDelaySeconds(), readOnly);


        fileCache = new FileCache(builder.getIntialFileCacheSize(), builder.getMaximumFileCacheSize(), readOnly, builder.getFileCacheExecutorService());
        blobPageCache = new PageCache(builder.getBlobPageSize(), builder.getInitialBlobCacheSize(), builder.getMaximumBlobCacheSize(), fileCache, builder.getBlobCacheExecutorService());
        PageCache lookupPageCache = new PageCache(builder.getLookupPageSize(), builder.getInitialLookupPageCacheSize(), builder.getMaximumLookupPageCacheSize(), fileCache, builder.getLookupPageCacheExecutorService());
        lookupCache = new LookupCache(lookupPageCache, builder.getInitialLookupKeyCacheSize(), builder.getMaximumLookupKeyCacheWeight(), builder.getInitialMetaDataCacheSize(), builder.getMaximumMetaDataCacheWeight(), builder.getLookupKeyCacheExecutorService(), builder.getLookupMetaDataCacheExecutorService());

        blocks = new BlockedLongs(builder.getDir().resolve("blocks"), builder.getBlobsPerBlock(), readOnly);

        openPartitionFunction = partitionKey -> AppendStorePartition.openPartition(partionPath(builder.getDir()), partitionKey, builder.getLookupHashSize(), blobPageCache, lookupCache);

        createPartitionFunction = partitionKey -> AppendStorePartition.createPartition(partionPath(builder.getDir()), partitionKey, builder.getLookupHashSize(), blobPageCache, lookupCache);
    }

    private static Path partionPath(Path dir){
        return dir.resolve("partitions");
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("appending for partition '{}', key '{}'", partition, key);
        if (readOnly) throw new RuntimeException("Can not append to store opened in read only mode:" + dir);
        getOrCreate(partition).append(key, value, blocks);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        log.trace("reading in partition {} with key {}", partition, key);

        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.read(key, blocks))
                .orElse(Stream.empty());
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        log.trace("reading sequential in partition {} with key {}", partition, key);
        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.readSequential(key, blocks))
                .orElse(Stream.empty());
    }

    public byte[] readLast(String partition, String key) {
        log.trace("reading last in partition {} with key {}", partition, key);
        return getIfPresent(partition)
                .map(partitionObject -> partitionObject.readLast(key, blocks))
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
                .map(partitionObject -> partitionObject.scan(blocks))
                .orElse(Stream.empty());
    }

    @Override
    public void scan(String partition, BiConsumer<String, Stream<byte[]>> callback) {
        getIfPresent(partition)
                .ifPresent(partitionObject -> partitionObject.scan(blocks, callback));
    }

    @Override
    public void clear() {
        // Consider using a ReadWrite lock for clear and close?
        if (readOnly) throw new RuntimeException("Can not clear a store opened in read only mode:" + dir);

        log.trace("clearing");
        blocks.clear();

        listPartitions(partionPath(dir))
                .map(this::getIfPresent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(AppendStorePartition::clear);
        partitionMap.clear();
        lookupCache.flush();
        blobPageCache.flush();
        fileCache.flush();
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
    protected void flushInternal() throws IOException {
        // Flush lookups, then blocks, then blobs, since this is the access order of a read.
        // Check non null because the super class is registered in the autoflusher before the constructor finishes
        if (readOnly) throw new RuntimeException("Can not flush a store opened in read only mode:" + dir);

        blocks.flush();
        for (AppendStorePartition appendStorePartition : partitionMap.values()){
            appendStorePartition.flush();
        }
    }

    @Override
    public void trimInternal() throws IOException {
        if (!readOnly) flushInternal();
        lookupCache.flush();
        blobPageCache.flush();
        fileCache.flush();
    }

    @Override
    protected void closeInternal() throws IOException {
        trimInternal();

        try {
            blocks.close();
        } catch (Exception e) {
            log.error("unable to close blocks", e);
        }
    }
}
