package com.upserve.uppend.metrics;

import com.codahale.metrics.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.CounterStore;
import com.upserve.uppend.lookup.FlushStats;

import java.util.Map;
import java.util.function.ObjLongConsumer;
import java.util.stream.Stream;

public class CounterStoreWithMetrics implements CounterStore {
    private final CounterStore store;
    private final MetricRegistry metrics;

    public static final String SET_TIMER_METRIC_NAME = "setTimer";
    public static final String INCREMENT_TIMER_METRIC_NAME = "incrementTimer";
    public static final String FLUSH_TIMER_METRIC_NAME = "flushTimer";
    public static final String GET_TIMER_METRIC_NAME = "getTimer";
    public static final String KEYS_TIMER_METRIC_NAME = "keysTimer";
    public static final String SCAN_TIMER_METRIC_NAME = "scanTimer";
    public static final String CLEAR_TIMER_METRIC_NAME = "clearTimer";
    public static final String CLOSE_TIMER_METRIC_NAME = "closeTimer";
    public static final String TRIM_TIMER_METRIC_NAME = "trimTimer";

    public static final String UPPEND_COUNTER_STORE = "uppendCounterStore";

    private final Timer metricsSetTimer;
    private final Timer metricsIncrementTimer;
    private final Timer metricsFlushTimer;
    private final Timer metricsGetTimer;
    private final Timer metricsKeysTimer;
    private final Timer metricsScanTimer;
    private final Timer metricsClearTimer;
    private final Timer metricsCloseTimer;
    private final Timer metricsTrimTimer;

    public CounterStoreWithMetrics(CounterStore store, MetricRegistry metrics, String rootName) {
        this.store = store;
        this.metrics = metrics;

        metricsSetTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), SET_TIMER_METRIC_NAME));
        metricsIncrementTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), INCREMENT_TIMER_METRIC_NAME));
        metricsFlushTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), FLUSH_TIMER_METRIC_NAME));
        metricsGetTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), GET_TIMER_METRIC_NAME));
        metricsKeysTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), KEYS_TIMER_METRIC_NAME));
        metricsScanTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), SCAN_TIMER_METRIC_NAME));
        metricsClearTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME));
        metricsCloseTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), CLOSE_TIMER_METRIC_NAME));
        metricsTrimTimer = metrics.timer(MetricRegistry.name(rootName, UPPEND_COUNTER_STORE, store.getName(), TRIM_TIMER_METRIC_NAME));
    }

    @Override
    public Long set(String partitionEntropy, String key, long value) {
        final Timer.Context context = metricsSetTimer.time();
        try {
            return store.set(partitionEntropy, key, value);
        } finally {
            context.stop();
        }
    }

    @Override
    public long increment(String partitionEntropy, String key, long delta) {
        final Timer.Context context = metricsIncrementTimer.time();
        try {
            return store.increment(partitionEntropy, key, delta);
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
        final Timer.Context context = metricsFlushTimer.time();
        try {
            store.flush();
        } finally {
            context.stop();
        }
    }

    @Override
    public Long get(String partitionEntropy, String key) {
        final Timer.Context context = metricsGetTimer.time();
        try {
            return store.get(partitionEntropy, key);
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> keys() {
        final Timer.Context context = metricsKeysTimer.time();
        try {
            return store.keys();
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<Map.Entry<String, Long>> scan() {
        final Timer.Context context = metricsScanTimer.time();
        try {
            return store.scan();
        } finally {
            context.stop();
        }
    }

    @Override
    public void scan(ObjLongConsumer<String> callback) {
        final Timer.Context context = metricsScanTimer.time();
        try {
            store.scan(callback);
        } finally {
            context.stop();
        }
    }

    @Override
    public FlushStats getFlushStats() {
        return store.getFlushStats();
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
    public long keyCount() {
        return store.keyCount();
    }

    @Override
    public void clear() {
        final Timer.Context context = metricsClearTimer.time();
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
    public void trim() {
        final Timer.Context context = metricsTrimTimer.time();
        try {
            store.trim();
        } finally {
            context.stop();
        }
    }

    @Override
    public void close() throws Exception {
        final Timer.Context context = metricsCloseTimer.time();
        try {
            store.close();
        } finally {
            context.stop();
        }
    }
}
