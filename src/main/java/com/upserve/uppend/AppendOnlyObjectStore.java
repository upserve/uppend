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
public class AppendOnlyObjectStore<T> implements AutoCloseable, RegisteredFlushable {
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
     * @param partitionEntropy the partition to store under
     * @param key the key to store under
     * @param value the value to append
     * @throws IllegalArgumentException if partition is invalid
     */
    public void append(String partitionEntropy, String key, T value) {
        store.append(partitionEntropy, key, serializer.apply(value));
    }

    /**
     * Read objects that have been stored under a given partition and key in
     * parallel
     *
     * @param partitionEntropy the key under which to retrieve
     * @param key the key under which to retrieve
     * @return a stream of the stored objects
     * @throws IllegalArgumentException if partition is invalid
     */
    public Stream<T> read(String partitionEntropy, String key) {
        return store.read(partitionEntropy, key).map(deserializer);
    }

    /**
     * Read objects that have been stored under a given partition and key in
     * the order they were stored
     *
     * @param partitionEntropy the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return a stream of the stored objects in storage order
     * @throws IllegalArgumentException if partition is invalid
     */
    public Stream<T> readSequential(String partitionEntropy, String key) {
        return store.readSequential(partitionEntropy, key).map(deserializer);
    }

    /**
     * Read the last object that was stored under a given partition and key
     *
     * @param partitionEntropy the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return the stored object, or null if none
     * @throws IllegalArgumentException if partition is invalid
     */
    public T readLast(String partitionEntropy, String key) {
        return deserializer.apply(store.readLast(partitionEntropy, key));
    }

    /**
     * Enumerate the keys in the data store
     *
     * @return a stream of string keys
     * @throws IllegalArgumentException if partition is invalid
     */
    public Stream<String> keys() {
        return store.keys();
    }

    /**
     * Scan all the keys and values, returning a stream of
     * entries for each key
     *
     * @return a stream of entries of key to stream of object values
     */
    public Stream<Map.Entry<String, Stream<T>>> scan() {
        return store.scan().map(entry ->
                Maps.immutableEntry(
                        entry.getKey(),
                        entry.getValue().map(deserializer)
                ));
    }

    /**
     * Scan all the keys and values, calling the given function
     * with each key and stream of object values
     *
     * @param callback function to call for each key and stream of values
     */
    public void scan(BiConsumer<String, Stream<T>> callback) {
        store.scan((key, byteValues) ->
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
    public void register(int seconds) {
        store.register(seconds);
    }

    @Override
    public void deregister() {
        store.deregister();
    }

    @Override
    public void flush() throws IOException {
        store.flush();
    }

    public void trim() {
        store.trim();
    }

    public String getName() {
        return store.getName();
    }

    public long keyCount() { return store.keyCount(); }

    public AppendOnlyStore getStore() {
        return store;
    }
}
