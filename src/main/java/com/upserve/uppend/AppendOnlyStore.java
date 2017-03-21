package com.upserve.uppend;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Defines the minimum interface required to add byte arrays under a key, and to
 * retrieve them. Note the expectation that the byte arrays are appended to the
 * value, which can be thought of as an ever-growing list of byte arrays.
 */
public interface AppendOnlyStore extends AutoCloseable {
    /**
     * Append a byte array under a given key
     *
     * @param key the key to store under
     * @param value the value to append
     */
    void append(String key, byte[] value);
    
    /**
     * Read byte arrays that have been stored under a given key
     *
     * @param key the key under which to retrieve
     * @param reader function to be called once per stored byte array
     */
    void read(String key, Consumer<byte[]> reader);

    /**
     * Read byte arrays that have been stored under a given key as a stream
     *
     * @param key the key under which to retrieve
     */
    Stream<byte[]> read(String key);

    /**
     * Stream of the keys in the append store instance
     *
     * @param prefix the key prefix for a set of stored objects. All keys if null.
     */
    Stream<String> scan(String prefix);

    /**
     * Remove all keys and values from the store.
     */
    void clear();
}