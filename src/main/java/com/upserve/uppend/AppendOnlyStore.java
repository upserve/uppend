package com.upserve.uppend;

import java.io.Flushable;
import java.util.stream.Stream;

/**
 * Add byte arrays under a key and partition, and retrieve them. Note the
 * expectation that the byte arrays are appended to the value, which is an
 * ever-growing list of byte arrays.
 */
public interface AppendOnlyStore extends ReadOnlyAppendOnlyStore, Trimmable, AutoCloseable, Flushable {
    /**
     * Append a byte array under a given partition and key
     *
     * @param partition the partition to store under
     * @param key       the key to store under
     * @param value     the value to append
     * @throws IllegalArgumentException if partition is invalid
     */
    void append(String partition, String key, byte[] value);

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
}