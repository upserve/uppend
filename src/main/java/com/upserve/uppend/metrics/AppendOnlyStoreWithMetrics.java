package com.upserve.uppend.metrics;

import com.codahale.metrics.*;
import com.google.common.collect.Maps;
import com.upserve.uppend.*;

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

    public static final String FLUSHED_KEY_COUNT_GAUGE_METRIC_NAME = "flushedKeyCountGauge";
    public static final String FLUSH_COUNT_GAUGE_METRIC_NAME = "flushCountGauge";
    public static final String FLUSH_TIMER_GAUGE_METRIC_NAME = "flushTimerGauge";
    public static final String LOOKUP_MISS_COUNT_GAUGE_METRIC_NAME = "lookupMissCountGauge";
    public static final String LOOKUP_HIT_COUNT_GAUGE_METRIC_NAME = "lookupHitCountGauge";
    public static final String CACHE_MISS_COUNT_GAUGE_METRIC_NAME = "cacheMissCountGauge";
    public static final String CACHE_HIT_COUNT_GAUGE_METRIC_NAME = "cacheHitCountGauge";
    public static final String FIND_KEY_TIMER_GAUGE_METRIC_NAME = "findKeyTimerGauge";
    public static final String AVG_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME = "avgLookupDataSizeGauge";
    public static final String MAX_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME = "maxLookupDataSizeGauge";
    public static final String SUM_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME = "sumLookupDataSizeGauge";

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

        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), FLUSHED_KEY_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getFlushedKeyCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), FLUSH_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getFlushCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), FLUSH_TIMER_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getFlushTimer);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), LOOKUP_MISS_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getLookupMissCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), LOOKUP_HIT_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getLookupHitCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), CACHE_MISS_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getCacheMissCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), CACHE_HIT_COUNT_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getCacheHitCount);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), FIND_KEY_TIMER_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getFindKeyTimer);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), AVG_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME),
                (Gauge<Double>) getLookupDataMetrics()::getAvgLookupDataSize);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), MAX_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getMaxLookupDataSize);
        metrics.register(
                MetricRegistry.name(rootName, UPPEND_APPEND_STORE, store.getName(), SUM_LOOKUP_DATA_SIZE_GAUGE_METRIC_NAME),
                (Gauge<Long>) getLookupDataMetrics()::getSumLookupDataSize);
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
            store.scan((key, vals) -> {
                scanKeysMeter.mark(1);
                callback.accept(key, vals.peek(bytes -> scanBytesMeter.mark(bytes.length)));
            });
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
    public BlockedLongMetrics getBlockedLongMetrics() {
        return store.getBlockedLongMetrics();
    }

    @Override
    public BlobStoreMetrics getBlobStoreMetrics() {
        return store.getBlobStoreMetrics();
    }

    @Override
    public LookupDataMetrics getLookupDataMetrics() {
        return store.getLookupDataMetrics();
    }

    @Override
    public LongBlobStoreMetrics getLongBlobStoreMetrics() {
        return store.getLongBlobStoreMetrics();
    }

    @Override
    public MutableBlobStoreMetrics getMutableBlobStoreMetrics() {
        return store.getMutableBlobStoreMetrics();
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
