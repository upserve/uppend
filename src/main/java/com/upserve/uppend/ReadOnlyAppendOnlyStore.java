package com.upserve.uppend;

import java.util.Map;
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
     * @throws IllegalArgumentException if partition is invalid
     * @return a parallel stream of the stored byte arrays
     */
    Stream<byte[]> read(String partition, String key);

    /**
     * Read byte arrays that have been stored under a given partition and key in
     * the order they were stored
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of the stored byte arrays in storage order
     */
    Stream<byte[]> readSequential(String partition, String key);

    /**
     * Read the last byte array that was stored under a given partition and key
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return the stored byte array, or null if none
     */
    byte[] readLast(String partition, String key);

    /**
     * Enumerate the keys for a given partition
     *
     * @param partition the partition under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of string keys
     */
    Stream<String> keys(String partition);

    /**
     * Enumerate the partitions in the data store
     *
     * @return a stream of string partition
     */
    Stream<String> partitions();

    /**
     * Scan the given partition returning a stream of the contents including the key
     * @param partition the partition to scan
     * @return a stream of entries containing the key and the bytes
     */
    Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition);

    /**
     * The approximate number of keys in the data store. Keys that have been written but not yet flushed are counted.
     *
     * @return the number of keys
     */
    long size();
}
