package com.upserve.uppend;

import java.io.Flushable;
import java.util.stream.Stream;

/**
 * Add byte arrays under a key and partition, and retrieve them. Note the
 * expectation that the byte arrays are appended to the value, which is an
 * ever-growing list of byte arrays.
 */
public interface AppendOnlyStore extends ReadOnlyAppendOnlyStore, Flushable {
    /**
     * Read byte arrays that have been stored under a given partition and key in
     * parallel, skipping the write cache of this store so that unflushed data
     * is not visible
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a parallel stream of the stored byte arrays
     */
    Stream<byte[]> readFlushed(String partition, String key);

    /**
     * Read byte arrays that have been stored under a given partition and key in
     * the order they were stored, skipping the write cache of this store so
     * that unflushed data is not visible
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return a stream of the stored byte arrays in storage order
     */
    Stream<byte[]> readSequentialFlushed(String partition, String key);

    /**
     * Read the last byte array that was stored under a given partition and
     * key, skipping the write cache of this store so that unflushed data is not
     * visible
     *
     * @param partition the partition under which to retrieve
     * @param key the key under which to retrieve
     * @throws IllegalArgumentException if partition is invalid
     * @return the stored byte array, or null if none
     */
    byte[] readLastFlushed(String partition, String key);

    /**
     * Append a byte array under a given partition and key
     *
     * @param partition the partition to store under
     * @param key the key to store under
     * @throws IllegalArgumentException if partition is invalid
     * @param value the value to append
     */
    void append(String partition, String key, byte[] value);

    /**
     * Flush any pending appends to durable storage. Will not return until
     * the flush is completed.
     */
    @Override
    void flush();

    /**
     * Purge the write cache
     */
    void purgeWriteCache();

    /**
     * Remove all keys and values from the store.
     */
    void clear();

    /**
     * Provide some insight into the resource used by the store
     * @return a string describing the resources used
     */
    AppendStoreStats cacheStats();
}