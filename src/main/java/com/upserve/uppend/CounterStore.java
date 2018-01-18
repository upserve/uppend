package com.upserve.uppend;

import java.io.Flushable;

/**
 * Keep counters for partitioned keys.
 */
public interface CounterStore extends ReadOnlyCounterStore, Flushable {
    /**
     * Set the counter under a given partition and key, to the given value
     *
     * @param partition the partition to increment under
     * @param key the key to increment under
     * @param value the value to set
     * @throws IllegalArgumentException if partition is invalid
     * @return the old value of the counter or 0 if it was previously unset
     */
    long set(String partition, String key, long value);

    /**
     * Increment by 1 the counter under a given partition and key, whose value
     * is initialized to 0
     *
     * @param partition the partition to increment under
     * @param key the key to increment under
     * @throws IllegalArgumentException if partition is invalid
     * @return the new value of the counter
     */
    default long increment(String partition, String key) {
        return increment(partition, key, 1);
    }

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
     * Remove all keys and values from the store.
     */
    void clear();

    /**
     * Provide some insight into the resource used by the store
     * @return a string describing the resources used
     */
    AppendStoreStats cacheStats();
}