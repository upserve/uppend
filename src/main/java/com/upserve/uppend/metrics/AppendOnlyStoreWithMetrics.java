package com.upserve.uppend.metrics;

import com.codahale.metrics.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.collect.Maps;
import com.upserve.uppend.*;
import com.upserve.uppend.lookup.FlushStats;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class AppendOnlyStoreWithMetrics implements AppendOnlyStore {
    public static final String WRITE_TIMER_METRIC_NAME = "writeTimer";
    public static final String FLUSH_TIMER_METRIC_NAME = "flushTimer";
    public static final String READ_TIMER_METRIC_NAME = "readTimer";
    public static final String KEYS_TIMER_METRIC_NAME = "keysTimer";
    public static final String SCAN_TIMER_METRIC_NAME = "scanTimer";
    public static final String CLEAR_TIMER_METRIC_NAME = "clearTimer";
    public static final String CLOSE_TIMER_METRIC_NAME = "closeTimer";
    public static final String TRIM_TIMER_METRIC_NAME = "purgeTimer";

    public static final String WRITE_BYTES_METER_METRIC_NAME = "writeBytesMeter";
    public static final String READ_BYTES_METER_METRIC_NAME = "readBytesMeter";
    public static final String SCAN_BYTES_METER_METRIC_NAME = "scanBytesMeter";
    public static final String SCAN_KEYS_METER_METRIC_NAME = "scanKeysMeter";

    public static final String UPPEND_APPEND_STORE = "uppendAppendStore";

    private final AppendOnlyStore store;
    private final MetricRegistry metrics;

    private final Timer writeTimer;
    private final Timer flushTimer;
    private final Timer readTimer;
    private final Timer keysTimer;
    private final Timer scanTimer;
    private final Timer clearTimer;
    private final Timer closeTimer;
    private final Timer trimTimer;

    private final Meter writeBytesMeter;
    private final Meter readBytesMeter;
    private final Meter scanBytesMeter;
    private final Meter scanKeysMeter;

    /**
     * Constructor for an Append only store with metrics wrapper
     * Metrics Registry Key structure will be: ROOT_NAME.uppendAppendStore.STORE_NAME.METRIC_NAME
     * @param store the append only store to wrap
     * @param metrics the metrics registry to use
     * @param rootName the root name for metrics from this store
     */
    public AppendOnlyStoreWithMetrics(AppendOnlyStore store, MetricRegistry metrics, String rootName) {
        this.store = store;
        this.metrics = metrics;

        writeTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), WRITE_TIMER_METRIC_NAME));
        flushTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), FLUSH_TIMER_METRIC_NAME));
        readTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME));
        keysTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), KEYS_TIMER_METRIC_NAME));
        scanTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), SCAN_TIMER_METRIC_NAME));
        clearTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME));
        closeTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), CLOSE_TIMER_METRIC_NAME));
        trimTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), TRIM_TIMER_METRIC_NAME));

        writeBytesMeter = metrics.meter(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), WRITE_BYTES_METER_METRIC_NAME));
        readBytesMeter = metrics.meter(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME));
        scanBytesMeter = metrics.meter(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), SCAN_BYTES_METER_METRIC_NAME));
        scanKeysMeter = metrics.meter(MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), SCAN_KEYS_METER_METRIC_NAME));
    }

    @Override
    public void append(String partitionEntropy, String key, byte[] value) {
        final Timer.Context context = writeTimer.time();
        try {
            writeBytesMeter.mark(value.length);
            store.append(partitionEntropy, key, value);
        } finally {
            context.stop();
        }
    }

    @Override
    public void register(int seconds) {
        store.register(seconds);
    }

    @Override
    public void deregister() {
        store.deregister();
    }

    @Override
    public void flush() {
        final Timer.Context context = flushTimer.time();
        try {
            store.flush();
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<byte[]> read(String partitionEntropy, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.read(partitionEntropy, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<byte[]> readSequential(String partitionEntropy, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.readSequential(partitionEntropy, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public byte[] readLast(String partitionEntropy, String key) {
        final Timer.Context context = readTimer.time();
        try {
            byte[] bytes = store.readLast(partitionEntropy, key);
            readBytesMeter.mark(bytes.length);
            return bytes;
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> keys() {
        final Timer.Context context = keysTimer.time();
        try {
            return store.keys();
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<Map.Entry<String, Stream<byte[]>>> scan() {
        final Timer.Context context = scanTimer.time();
        try {
            return store.scan()
                    .peek(entry -> scanKeysMeter.mark(1))
                    .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().peek(bytes -> scanBytesMeter.mark(bytes.length))));
        } finally {
            context.stop();
        }
    }

    @Override
    public void scan(BiConsumer<String, Stream<byte[]>> callback) {
        final Timer.Context context = scanTimer.time();
        try {
            store.scan(callback);
        } finally {
            context.stop();
        }
    }

    @Override
    public void clear() {
        final Timer.Context context = clearTimer.time();
        try {
            store.clear();
        } finally {
            context.stop();
        }
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public FlushStats getFlushStats() {
        return store.getFlushStats();
    }

    @Override
    public CacheStats getBlobPageCacheStats() {
        return store.getBlobPageCacheStats();
    }

    @Override
    public CacheStats getKeyPageCacheStats() {
        return store.getKeyPageCacheStats();
    }

    @Override
    public CacheStats getLookupKeyCacheStats() {
        return store.getLookupKeyCacheStats();
    }

    @Override
    public CacheStats getMetadataCacheStats() {
        return store.getMetadataCacheStats();
    }

    @Override
    public BlockStats getBlockLongStats() {
        return store.getBlockLongStats();
    }

    @Override
    public long keyCount() {
        return store.keyCount();
    }

    @Override
    public void trim() {
        final Timer.Context context = trimTimer.time();
        try {
            store.trim();
        } finally {
            context.stop();
        }
    }

    @Override
    public void close() throws Exception {
        final Timer.Context context = closeTimer.time();
        try {
            store.close();
        } finally {
            context.stop();
        }
    }
}
