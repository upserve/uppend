package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.lookup.FlushStats;

import java.io.Flushable;

/**
 * Add byte arrays under a key and partition, and retrieve them. Note the
 * expectation that the byte arrays are appended to the value, which is an
 * ever-growing list of byte arrays.
 */
public interface AppendOnlyStore extends ReadOnlyAppendOnlyStore, RegisteredFlushable {
    /**
     * Append a byte array under a given partition and key
     *
     * @param partitionEntropy the partition to store under
     * @param key the key to store under
     * @param value the value to append
     * @throws IllegalArgumentException if partition is invalid
     */
    void append(String partitionEntropy, String key, byte[] value);

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
     * Get the name of this store - the last element in the path
     *
     * @return the name
     */
    String getName();

    FlushStats getFlushStats();
}