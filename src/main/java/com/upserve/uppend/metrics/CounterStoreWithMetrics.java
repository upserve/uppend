package com.upserve.uppend.metrics;

import com.codahale.metrics.*;
import com.upserve.uppend.*;

import java.util.stream.Stream;

public class CounterStoreWithMetrics implements CounterStore {
    private final CounterStore store;
    private final MetricRegistry metrics;

    private final Timer metricsSetTimer;
    private final Timer metricsIncrementTimer;
    private final Timer metricsFlushTimer;
    private final Timer metricsGetTimer;
    private final Timer metricsKeysTimer;
    private final Timer metricsPartitionsTimer;
    private final Timer metricsClearTimer;
    private final Timer metricsCloseTimer;

    public CounterStoreWithMetrics(CounterStore store, MetricRegistry metrics) {
        this.store = store;
        this.metrics = metrics;

        metricsSetTimer = metrics.timer("set");
        metricsIncrementTimer = metrics.timer("increment");
        metricsFlushTimer = metrics.timer("flush");
        metricsGetTimer = metrics.timer("get");
        metricsKeysTimer = metrics.timer("keys");
        metricsPartitionsTimer = metrics.timer("partitions");
        metricsClearTimer = metrics.timer("clear");
        metricsCloseTimer = metrics.timer("close");
    }

    @Override
    public long set(String partition, String key, long value) {
        final Timer.Context context = metricsSetTimer.time();
        try {
            return store.set(partition, key, value);
        } finally {
            context.stop();
        }
    }

    @Override
    public long increment(String partition, String key, long delta) {
        final Timer.Context context = metricsIncrementTimer.time();
        try {
            return store.increment(partition, key, delta);
        } finally {
            context.stop();
        }
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
    public long get(String partition, String key) {
        final Timer.Context context = metricsGetTimer.time();
        try {
            return store.get(partition, key);
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> keys(String partition) {
        final Timer.Context context = metricsKeysTimer.time();
        try {
            return store.keys(partition);
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> partitions() {
        final Timer.Context context = metricsPartitionsTimer.time();
        try {
            return store.partitions();
        } finally {
            context.stop();
        }
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
    public AppendStoreStats cacheStats() {
        return store.cacheStats();
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
