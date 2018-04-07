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

public class CounterStorePartition extends Partition implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path partitiondDir;
    private final LongLookup lookups;

    public static CounterStorePartition createPartition(Path partentDir, String partition, int hashSize, LookupCache lookupCache){
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        return new CounterStorePartition(
                partitiondDir,
                new LongLookup(lookupsDir(partitiondDir), hashSize, PartitionLookupCache.create(partition, lookupCache))
        );
    }

    public static CounterStorePartition openPartition(Path partentDir, String partition, int hashSize, LookupCache lookupCache) {
        validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        Path lookupsDir = lookupsDir(partitiondDir);

        if (Files.exists(lookupsDir)) {
            return new CounterStorePartition(
                    partitiondDir,
                    new LongLookup(lookupsDir, hashSize, PartitionLookupCache.create(partition, lookupCache))
            );
        } else {
            return null;
        }
    }

    private CounterStorePartition(Path partentDir, LongLookup longLookup){
        partitiondDir = partentDir;
        this.lookups = longLookup;
    }

    public long set(String key, long value) {
        return lookups.put(key, value);
    }

    public long increment(String key, long delta) {
        return lookups.increment(key, delta);
    }

    public Long get(String key) {
       return lookups.getLookupData(key);
    }

    public Stream<Map.Entry<String, Long>> scan() {
        return lookups.scan();
    }

    public void scan(ObjLongConsumer<String> callback) {
        lookups.scan(callback);
    }

    Stream<String> keys(){
        return lookups.keys();
    }

    @Override
    public void flush() throws IOException {
        lookups.flush();
    }

    void clear(){
        lookups.clear();
    }
}
