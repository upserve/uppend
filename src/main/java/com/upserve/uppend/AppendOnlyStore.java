package com.upserve.uppend;

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
     * @param partition the partition to store under
     * @param key the key to store under
     * @throws IllegalArgumentException if partition is invalid
     * @param value the value to append
     */
    void append(String partition, String key, byte[] value);

    /**
     * Read byte arrays that have been stored under a given key
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of the stored byte arrays
     */
    Stream<byte[]> read(String partition, String key);

    /**
     * Read the last byte array that was stored under a given key
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return the stored byte array, or null if none
     */
    byte[] readLast(String partition, String key);

    /**
     * Enumerate the keys in the data store
     *
     * @param partition the partition under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of string keys
     */
    Stream<String> keys(String partition);

    /**
     * Enumerate the partition in the data store
     *
     * @return a stream of string partition
     */
    Stream<String> partitions();

    /**
     * Remove all keys and values from the store.
     */
    void clear();
}