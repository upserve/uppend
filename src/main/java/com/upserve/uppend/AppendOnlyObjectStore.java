package com.upserve.uppend;

import com.google.common.collect.Maps;

import java.io.*;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Stream;

/**
 * Wraps an {@code AppendOnlyStore} and {@code Serializer} and provides
 * convenience typed-access to the deserialized form in an append-only
 * multi-map.
 */
public class AppendOnlyObjectStore<T> implements AutoCloseable, Flushable {
    private final AppendOnlyStore store;
    private final Function<T, byte[]> serializer;
    private final Function<byte[], T> deserializer;

    /**
     * Constructs new instance, wrapping the underlying {@code AppendOnlyStore},
     * and using the supplied serialization/deserialization functions.
     *
     * @param store the append-only store to keep the serialized byte arrays
     * @param serializer the serialization function
     * @param deserializer the deserialization function
     */
    public AppendOnlyObjectStore(AppendOnlyStore store, Function<T, byte[]> serializer, Function<byte[], T> deserializer) {
        this.store = store;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * Append an object under a given key
     *
     * @param partition the partition to store under
     * @param key the key to store under
     * @param value the value to append
     * @throws IllegalArgumentException if partition is invalid
     */
    public void append(String partition, String key, T value) {
        store.append(partition, key, serializer.apply(value));
    }

    /**
     * Read objects that have been stored under a given partition and key in
     * parallel
     *
     * @param partition the key under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of the stored objects
     */
    public Stream<T> read(String partition, String key) {
        return store.read(partition, key).map(deserializer);
    }

    /**
     * Read objects that have been stored under a given partition and key in
     * the order they were stored
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of the stored objects in storage order
     */
    public Stream<T> readSequential(String partition, String key) {
        return store.readSequential(partition, key).map(deserializer);
    }

    /**
     * Read the last object that was stored under a given partition and key
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return the stored object, or null if none
     */
    public T readLast(String partition, String key) {
        return deserializer.apply(store.readLast(partition, key));
    }

    /**
     * Enumerate the keys in the data store
     *
     * @param partition the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of string keys
     */
    public Stream<String> keys(String partition) {
        return store.keys(partition);
    }

    /**
     * Enumerate the partitions in the data store
     *
     * @return a stream of string partitions
     */
    public Stream<String> partitions() {
        return store.partitions();
    }

    /**
     * Scan all the keys and values in a partition, returning a stream of
     * entries for each key
     *
     * @param partition the name of the partition
     * @return a stream of entries of key to stream of object values
     */
    public Stream<Map.Entry<String, Stream<T>>> scan(String partition) {
        return store.scan(partition).map(entry ->
                Maps.immutableEntry(
                        entry.getKey(),
                        entry.getValue().map(deserializer)
                ));
    }

    /**
     * Scan all the keys and values in a partition, calling the given function
     * with each key and stream of object values
     *
     * @param partition the name of the partition
     * @param callback function to call for each key and stream of values
     */
    public void scan(String partition, BiConsumer<String, Stream<T>> callback) {
        store.scan(partition, (key, byteValues) ->
                callback.accept(key, byteValues.map(deserializer))
        );
    }

    /**
     * Remove all keys and values from the store.
     */
    public void clear() {
        store.clear();
    }

    @Override
    public void close() throws Exception {
        store.close();
    }

    @Override
    public void flush() throws IOException {
        store.flush();
    }

    public void trim() {
        store.trim();
    }
}
