package com.upserve.uppend;

import java.io.Flushable;
import java.util.stream.Stream;

/**
 * Defines the minimum interface required to increment values under a key, and
 * to retrieve those values.
 */
public interface IncrementOnlyStore extends AutoCloseable, Flushable {
    /**
     * Increment by 1 the counter under a given partition and key, whose value
     * is initialized to 0
     *
     * @param partition the partition to increment under
     * @param key the key to increment under
     * @throws IllegalArgumentException if partition is invalid
     * @return the new value of the counter
     */
    long increment(String partition, String key);

    /**
     * Increment by a given amount the counter under a given partition and key,
     * whose value is initialized to 0
     *
     * @param partition the partition to increment under
     * @param key the key to increment under
     * @param delta the amount to add to the current value
     * @throws IllegalArgumentException if partition is invalid
     * @return the new value of the counter
     */
    long increment(String partition, String key, long delta);

    /**
     * Flush any pending appends to durable storage. Will not return until
     * the flush is completed.
     */
    @Override
    void flush();

    /**
     * Get the value for a given partition and key
     *
     * @param partition the partition to get
     * @param key the key to get
     * @throws IllegalArgumentException if partition is invalid
     * @return the value for the given partition and key, or 0 if not found
     */
    long get(String partition, String key);

    /**
     * Enumerate the keys for a given partition
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