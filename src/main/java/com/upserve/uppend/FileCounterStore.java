package com.upserve.uppend;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

public class FileCounterStore extends FileStore<CounterStorePartition> implements CounterStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Function<String, CounterStorePartition> openPartitionFunction;
    private final Function<String, CounterStorePartition> createPartitionFunction;

    FileCounterStore(boolean readOnly, CounterStoreBuilder builder) {
        super(builder.getDir(), builder.getFlushDelaySeconds(), builder.getPartitionCount(), readOnly, builder.getStoreName());


        openPartitionFunction = partitionKey -> CounterStorePartition.openPartition(partitionsDir, partitionKey, builder.getLookupHashCount(), builder.getTargetBufferSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getLookupPageSize(), readOnly);
        createPartitionFunction = partitionKey -> CounterStorePartition.createPartition(partitionsDir, partitionKey, builder.getLookupHashCount(), builder.getTargetBufferSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getLookupPageSize());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Long set(String partitionEntropy, String key, long value) {
        log.trace("setting {}={} in partition '{}'", key, value, partitionEntropy);
        if (readOnly) throw new RuntimeException("Can not set value of counter store opened in read only mode:" + dir);
        return getOrCreate(partitionEntropy).set(key, value);
    }

    @Override
    public long increment(String partitionEntropy, String key, long delta) {
        log.trace("incrementing by {} key '{}' in partition '{}'", delta, key, partitionEntropy);
        if (readOnly)
            throw new RuntimeException("Can not increment value of counter store opened in read only mode:" + dir);
        return getOrCreate(partitionEntropy).increment(key, delta);
    }

    @Override
    public Long get(String partitionEntropy, String key) {
        log.trace("getting value for key '{}' in partition '{}'", key, partitionEntropy);
        return getIfPresent(partitionEntropy).map(partitionObject -> partitionObject.get(key)).orElse(null);
    }

    @Override
    public Stream<String> keys() {
        log.trace("getting keys in {}", getName());
        return streamPartitions()
                .flatMap(CounterStorePartition::keys);
    }

    @Override
    public Stream<Map.Entry<String, Long>> scan() {
        return streamPartitions()
                .flatMap(CounterStorePartition::scan);
    }

    @Override
    public void scan(ObjLongConsumer<String> callback) {
        streamPartitions()
                .forEach(partitionObject -> partitionObject.scan(callback));
    }

    @Override
    public long keyCount() {
        return streamPartitions()
                .mapToLong(CounterStorePartition::keyCount)
                .sum();
    }

    @Override
    Function<String, CounterStorePartition> getOpenPartitionFunction() {
        return openPartitionFunction;
    }

    @Override
    Function<String, CounterStorePartition> getCreatePartitionFunction() {
        return createPartitionFunction;
    }

}
