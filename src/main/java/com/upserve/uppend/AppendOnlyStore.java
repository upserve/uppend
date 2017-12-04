package com.upserve.uppend;

import java.io.Flushable;

/**
 * Add byte arrays under a key and partition, and retrieve them. Note the
 * expectation that the byte arrays are appended to the value, which is an
 * ever-growing list of byte arrays.
 */
public interface AppendOnlyStore extends ReadOnlyAppendOnlyStore, Flushable {
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
     * Remove all keys and values from the store.
     */
    void clear();
}