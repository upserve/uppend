package com.upserve.uppend;

import com.upserve.uppend.util.Partition;
import com.upserve.uppend.util.Reservation;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class InMemoryAppendOnlyStore implements AppendOnlyStore {

    // Data for all In Memory Only Append Stores live in inMemoryStores.
    private static final Map<String, Map<String, Map<String, List<byte[]>>>> inMemoryStores = new ConcurrentHashMap<>();
    // Nested Map structure keys are: append-store-name (partition (key))

    // Used to reserve a single slot for each AppendOnlyStore in the JVM - there can be only one
    private static final Reservation reservation = new Reservation();

    // The nested map structure for each instance of the append store
    private final Map<String, Map<String, List<byte[]>>> partitionMap;

    // Default empty values used in read only operations
    private final Map<String, List<byte[]>> defaultKeyMap = new HashMap<>();
    private final List<byte[]> defaultList = new ArrayList<>();

    private final String pathString;

    public InMemoryAppendOnlyStore(Path path) {
        this(path.toString());
    }

    public InMemoryAppendOnlyStore(String pathString) {
        this.pathString = pathString;

        if (!reservation.checkout(uniqueStoreId(), this)) throw new IllegalStateException(
                String.format("An instance of append only store with this path already exists in the JVM: '%s'", pathString)
        );

        partitionMap = inMemoryStores.compute(pathString, (key, value) ->
                (value == null) ? new ConcurrentHashMap<>() : value);
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        Partition.validate(partition);
        partitionMap
                .compute(partition, (pKey, pValue) -> (pValue == null) ? new ConcurrentHashMap<>() : pValue)
                .compute(key, (kKey, kValue) -> (kValue == null) ? new ArrayList<>() : kValue)
                .add(value);
    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        Partition.validate(partition);
        return partitionMap.getOrDefault(partition, defaultKeyMap).getOrDefault(key, defaultList).stream();
    }

    @Override
    public Stream<String> keys(String partition) {
        Partition.validate(partition);
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
        reservation.checkin(uniqueStoreId(), this);
    }

    @Override
    public String uniqueStoreId(){
        return String.format("%s:%s",this.getClass(), pathString);
    }
}
