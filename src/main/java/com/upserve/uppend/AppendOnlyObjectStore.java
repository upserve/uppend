package com.upserve.uppend;

import java.util.function.*;

/**
 * Wraps an {@code AppendOnlyStore} and {@code Serializer} and provides
 * convenience typed-access to the deserialized form in an append-only
 * multi-map.
 */
public class AppendOnlyObjectStore<T> implements AutoCloseable {
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
     * @param key the key to store under
     * @param value the value to append
     */
    public void append(String key, T value) {
        store.append(key, serializer.apply(value));
    }

    /**
     * Read byte arrays that have been stored under a given key
     *
     * @param key the key under which to retrieve
     * @param reader function to be called once per stored byte array
     */
    public void read(String key, Consumer<T> reader) {
        store.read(key, bytes -> reader.accept(deserializer.apply(bytes)));
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
}