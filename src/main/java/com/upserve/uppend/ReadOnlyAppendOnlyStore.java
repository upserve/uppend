package com.upserve.uppend;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Reader interface to an append-only store
 */
public interface ReadOnlyAppendOnlyStore extends AutoCloseable {
    /**
     * Read byte arrays that have been stored under a given partition and key in
     * parallel
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return a parallel stream of the stored byte arrays
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<byte[]> read(String partition, String key);

    /**
     * Read byte arrays that have been stored under a given partition and key in
     * the order they were stored
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return a stream of the stored byte arrays in storage order
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<byte[]> readSequential(String partition, String key);


    /**
     * Read the last byte array that was stored under a given partition and key
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return the stored byte array, or null if none
     * @throws IllegalArgumentException if partition is invalid
     */
    byte[] readLast(String partition, String key);

    /**
     * Enumerate the keys for a given partition
     *
     * @param partition the partition under which to retrieve
     * @return a stream of string keys
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<String> keys(String partition);

    /**
     * Enumerate the partitions in the data store
     *
     * @return a stream of string partition
     */
    Stream<String> partitions();

    /**
     * Scan all the keys and values in a partition, returning a stream of
     * entries for each key
     *
     * @param partition the partition to scan
     * @return a stream of entries of key to stream of byte array values
     */
    Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition);

    /**
     * Scan the given partition, calling the given function with each key and
     * stream of byte array values
     *
     * @param partition the partition to scan
     * @param callback function to call for each key and stream of values
     */
    void scan(String partition, BiConsumer<String, Stream<byte[]>> callback);
}
