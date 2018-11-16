package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Map;
import java.util.function.ObjLongConsumer;
import java.util.stream.Stream;

public interface ReadOnlyCounterStore extends Trimmable, AutoCloseable {
    /**
     * Get the value for a given partition and key
     *
     * @param partitionEntropy the partition to getLookupData
     * @param key the key to getLookupData
     * @return the value for the given partition and key, or Null if not found
     * @throws IllegalArgumentException if partition is invalid
     */
    Long get(String partitionEntropy, String key);

    /**
     * Enumerate the keys in the counterStore
     *
     * @return a stream of string keys
     * @throws IllegalArgumentException if partition is invalid
     */
    Stream<String> keys();

    /**
     * Scan all the keys and values, returning a stream of
     * entries
     *
     * @return a stream of entries of key to counter values
     */
    Stream<Map.Entry<String, Long>> scan();

    /**
     * Scan the counter store, calling the given function with each key and
     * counter value
     *
     * @param callback function to call for each key and value
     */
    void scan(ObjLongConsumer<String> callback);

    long keyCount();
}
