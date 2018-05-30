package com.upserve.uppend;

import com.upserve.uppend.lookup.FlushStats;

import java.io.Flushable;

/**
 * Keep counters for partitioned keys.
 */
public interface CounterStore extends ReadOnlyCounterStore, RegisteredFlushable {
    /**
     * Set the counter under a given partition and key, to the given value
     *
     * @param partitionEntropy the partition to increment under
     * @param key the key to increment under
     * @param value the value to set
     * @return the old value of the counter or Null if it was previously unset
     * @throws IllegalArgumentException if partition is invalid
     */
    Long set(String partitionEntropy, String key, long value);

    /**
     * Increment by 1 the counter under a given partition and key, whose value
     * is initialized to 0
     *
     * @param partitionEntropy the partition to increment under
     * @param key the key to increment under
     * @return the new value of the counter
     * @throws IllegalArgumentException if partition is invalid
     */
    default long increment(String partitionEntropy, String key) {
        return increment(partitionEntropy, key, 1);
    }

    /**
     * Increment by a given amount the counter under a given partition and key,
     * whose value is initialized to 0
     *
     * @param partitionEntropy the partition to increment under
     * @param key the key to increment under
     * @param delta the amount to add to the current value
     * @return the new value of the counter
     * @throws IllegalArgumentException if partition is invalid
     */
    long increment(String partitionEntropy, String key, long delta);

    /**
     * Flush any pending appends to durable storage. Will not return until
     * the flush is completed.
     */
    @Override
    void flush();

    /**
     * Trim flushes any pending changes and then close cached resources to
     * reduce heap consumption
     */
    @Override
    void trim();

    /**
     * Remove all keys and values from the store.
     */
    void clear();

    /**
     * getValue the name of the counter store - the last element in the path
     *
     * @return the name of the datastore for reporting purposes
     */
    String getName();

    FlushStats getFlushStats();
}
