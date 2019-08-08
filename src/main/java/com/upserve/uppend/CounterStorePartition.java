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

public class CounterStorePartition extends Partition {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static CounterStorePartition createPartition(Path parentDir, String partition, CounterStoreBuilder builder) {
        Path partitionDir = validatePartition(parentDir, partition);

        VirtualPageFile metadata = new VirtualPageFile(
                metadataPath(partitionDir),
                builder.getLookupHashCount(),
                builder.getMetadataPageSize(),
                adjustedTargetBufferSize(
                        builder.getMetadataPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()
                ),
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

        return new CounterStorePartition(keys, metadata, false, builder);
    }

    static CounterStorePartition openPartition(Path partentDir, String partition, boolean readOnly, CounterStoreBuilder builder) {
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);

        if (!(Files.exists(metadataPath(partitiondDir)) && Files.exists(keysPath(partitiondDir)))) return null;

        VirtualPageFile metadata = new VirtualPageFile(
                metadataPath(partitiondDir),
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
                keysPath(partitiondDir),
                builder.getLookupHashCount(),
                builder.getLookupPageSize(),
                adjustedTargetBufferSize(
                        builder.getLookupPageSize(),
                        builder.getLookupHashCount(),
                        builder.getTargetBufferSize()
                ),
                readOnly);

        return new CounterStorePartition(keys, metadata, readOnly, builder);
    }

    private CounterStorePartition(VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, boolean readOnly, CounterStoreBuilder builder) {
        super(longKeyFile, metadataBlobFile, readOnly, builder);
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
        return IntStream.range(0, hashCount)
                .parallel()
                .boxed()
                .flatMap(virtualFileNumber -> lookups[virtualFileNumber].scan().map(entry -> Maps.immutableEntry(entry.getKey().string(), entry.getValue())));
    }

    public void scan(ObjLongConsumer<String> callback) {

        IntStream.range(0, hashCount)
                .parallel()
                .boxed()
                .forEach(virtualFileNumber -> lookups[virtualFileNumber].scan((keyLookup, value) -> callback.accept(keyLookup.string(), value)));
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
        SafeDeleting.removeDirectory(getLongKeyFile().getFilePath().getParent());
    }
}
