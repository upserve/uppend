package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Reader interface to an append-only store
 */
public interface ReadOnlyAppendOnlyStore extends Trimmable, AutoCloseable {
    /**
     * Read byte arrays that have been stored under a given partition and key in
     * parallel
     *
     * @param partitionEntropy the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return a parallel stream of the stored byte arrays
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<byte[]> read(String partitionEntropy, String key);

    /**
     * Read byte arrays that have been stored under a given partition and key in
     * the order they were stored
     *
     * @param partitionEntropy the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return a stream of the stored byte arrays in storage order
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<byte[]> readSequential(String partitionEntropy, String key);


    /**
     * Read the last byte array that was stored under a given partition and key
     *
     * @param partitionEntropy the partition under which to retrieve
     * @param key the key under which to retrieve
     * @return the stored byte array, or null if none
     * @throws IllegalArgumentException if partition is invalid
     */
    byte[] readLast(String partitionEntropy, String key);

    /**
     * Enumerate the keys in the append store
     *
     * @return a stream of string keys
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<String> keys();


    /**
     * Scan all the keys and values in the append store, returning a stream of
     * entries for each key and stream of values
     *
     * @return a stream of entries of key to stream of byte array values
     */
    Stream<Map.Entry<String, Stream<byte[]>>> scan();

    /**
     * Scan the append store, calling the given function with each key and
     * stream of byte array values
     *
     * @param callback function to call for each key and stream of values
     */
    void scan(BiConsumer<String, Stream<byte[]>> callback);

    CacheStats getLookupKeyCacheStats();

    BlockStats getBlockLongStats();

    long keyCount();
}
