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
import java.util.function.ObjLongConsumer;
import java.util.stream.*;

public class CounterStorePartition extends Partition implements Flushable, Closeable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static CounterStorePartition createPartition(Path partentDir, String partition, int hashSize, int flushThreshold, int reloadInterval, int metadataPageSize, int keyPageSize, LookupCache lookupCache) {
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        try {
            Files.createDirectories(partitiondDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to make partition directory: " + partitiondDir, e);
        }

        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitiondDir), hashSize, metadataPageSize, false);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitiondDir), hashSize, keyPageSize, false);


        return new CounterStorePartition(keys, metadata, PartitionLookupCache.create(partition, lookupCache), hashSize, flushThreshold, reloadInterval, false);
    }

    public static CounterStorePartition openPartition(Path partentDir, String partition, int hashSize, int flushThreshold, int reloadInterval, int metadataPageSize, int keyPageSize, LookupCache lookupCache, boolean readOnly) {
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);

        if (!(Files.exists(metadataPath(partitiondDir)) && Files.exists(keysPath(partitiondDir)))) return null;


        VirtualPageFile metadata = new VirtualPageFile(metadataPath(partitiondDir), hashSize, metadataPageSize, readOnly);
        VirtualPageFile keys = new VirtualPageFile(keysPath(partitiondDir), hashSize, keyPageSize, readOnly);

        return new CounterStorePartition(keys, metadata, PartitionLookupCache.create(partition, lookupCache), hashSize, flushThreshold, reloadInterval, false);
    }

    private CounterStorePartition(VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, PartitionLookupCache lookupCache, int hashSize, int flushThreshold, int reloadInterval, boolean readOnly) {
        super(longKeyFile, metadataBlobFile, lookupCache, hashSize, flushThreshold, reloadInterval, readOnly);
    }

    public Long set(String key, long value) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        return lookups[hash].put(lookupKey, value);
    }

    public long increment(String key, long delta) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        return lookups[hash].increment(lookupKey, delta);
    }

    public Long get(String key) {
        LookupKey lookupKey = new LookupKey(key);
        final int hash = keyHash(lookupKey);

        return lookups[hash].getValue(lookupKey);
    }

    public Stream<Map.Entry<String, Long>> scan() {
        return IntStream.range(0, hashSize)
                .parallel()
                .boxed()
                .flatMap(virtualFileNumber -> lookups[virtualFileNumber].scan().map(entry -> Maps.immutableEntry(entry.getKey().string(), entry.getValue())));
    }

    public void scan(ObjLongConsumer<String> callback) {

        IntStream.range(0, hashSize)
                .parallel()
                .boxed()
                .forEach(virtualFileNumber -> lookups[virtualFileNumber].scan((keyLookup, value) -> callback.accept(keyLookup.string(), value)));
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
        log.debug("Finished flush for partition: {}", lookupCache.getPartition());
    }

    void clear() throws IOException {
        longKeyFile.close();
        metadataBlobFile.close();
        SafeDeleting.removeDirectory(longKeyFile.getFilePath().getParent());
    }

    @Override
    public void close() throws IOException {
        flush();

        longKeyFile.close();
        metadataBlobFile.close();
    }
}
