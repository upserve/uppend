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
     * @return a stream of the stored byte arrays
     */
    Stream<byte[]> read(String key);

    /**
     * Enumerate keys in the data store
     *
     * @return a stream of string keys
     */
    Stream<String> keys();

    /**
     * Remove all keys and values from the store.
     */
    void clear();
}