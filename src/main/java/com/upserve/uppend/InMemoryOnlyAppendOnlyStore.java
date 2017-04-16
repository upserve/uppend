package com.upserve.uppend;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public class InMemoryOnlyAppendOnlyStore implements AppendOnlyStore {

    private static final Map<String, Map<String, Map<String, List<byte[]>>>> inMemoryStores = new ConcurrentHashMap<>();

    private final Map<String, Map<String, List<byte[]>>> partitionMap;
    private final Map<String, List<byte[]>> defaultKeyMap = new HashMap<>();
    private final List<byte[]> defaultList = new ArrayList<>();

    public InMemoryOnlyAppendOnlyStore(Path path) {
        this(path.toString());
    }

    public InMemoryOnlyAppendOnlyStore(String pathString) {
        partitionMap = inMemoryStores.compute(pathString, (key, value) -> {
            if (value == null) {
                value = new ConcurrentHashMap<>();
            }
            return value;
        });
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        FileAppendOnlyStore.validatePartition(partition);
        Map<String, List<byte[]>> keyMap = partitionMap.compute(partition, (pKey, pValue) -> {
            if (pValue == null) {
                pValue = new ConcurrentHashMap<>();
            }
            return pValue;
        });

        keyMap.compute(key, (kKey, kValue) -> {
            if (kValue == null) {
                kValue = new ArrayList<>();
            }
            kValue.add(value);
            return kValue;
        });
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        return partitionMap.getOrDefault(partition, defaultKeyMap).getOrDefault(key, defaultList).stream();
    }

    @Override
    public Stream<String> keys(String partition) {
        return partitionMap.getOrDefault(partition, defaultKeyMap).keySet().stream();
    }

    @Override
    public Stream<String> partitions() {
        return partitionMap.keySet().stream();
    }

    @Override
    public void clear() {
        partitionMap.clear();
    }

    @Override
    public void close() throws Exception {
        // noop
    }
}
