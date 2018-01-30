package com.upserve.uppend.metrics;

import com.codahale.metrics.*;
import com.upserve.uppend.*;

import java.util.Map;
import java.util.stream.Stream;

public class AppendOnlyStoreWithMetrics implements AppendOnlyStore {
    public static final String WRITE_TIMER_METRIC_NAME = "writeTimer";
    public static final String FLUSH_TIMER_METRIC_NAME = "flushTimer";
    public static final String READ_TIMER_METRIC_NAME = "readTimer";
    public static final String KEYS_TIMER_METRIC_NAME = "keysTimer";
    public static final String PARTITIONS_TIMER_METRIC_NAME = "partitionsTimer";
    public static final String CLEAR_TIMER_METRIC_NAME = "clearTimer";
    public static final String CLOSE_TIMER_METRIC_NAME = "closeTimer";
    public static final String SIZE_TIMER_METRIC_NAME = "sizeTimer";
    public static final String PURGE_TIMER_METRIC_NAME = "purgeTimer";

    public static final String WRITE_BYTES_METER_METRIC_NAME = "writeBytesMeter";
    public static final String READ_BYTES_METER_METRIC_NAME = "readBytesMeter";

    private final AppendOnlyStore store;
    private final MetricRegistry metrics;

    private final Timer writeTimer;
    private final Timer flushTimer;
    private final Timer readTimer;
    private final Timer keysTimer;
    private final Timer partitionsTimer;
    private final Timer clearTimer;
    private final Timer closeTimer;
    private final Timer sizeTimer;
    private final Timer purgeTimer;

    private final Meter writeBytesMeter;
    private final Meter readBytesMeter;

    public AppendOnlyStoreWithMetrics(AppendOnlyStore store, MetricRegistry metrics) {
        this.store = store;
        this.metrics = metrics;

        writeTimer = metrics.timer(WRITE_TIMER_METRIC_NAME);
        flushTimer = metrics.timer(FLUSH_TIMER_METRIC_NAME);
        readTimer = metrics.timer(READ_TIMER_METRIC_NAME);
        keysTimer = metrics.timer(KEYS_TIMER_METRIC_NAME);
        partitionsTimer = metrics.timer(PARTITIONS_TIMER_METRIC_NAME);
        clearTimer = metrics.timer(CLEAR_TIMER_METRIC_NAME);
        closeTimer = metrics.timer(CLOSE_TIMER_METRIC_NAME);
        sizeTimer = metrics.timer(SIZE_TIMER_METRIC_NAME);
        purgeTimer = metrics.timer(PURGE_TIMER_METRIC_NAME);

        writeBytesMeter = metrics.meter(WRITE_BYTES_METER_METRIC_NAME);
        readBytesMeter = metrics.meter(READ_BYTES_METER_METRIC_NAME);
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        final Timer.Context context = writeTimer.time();
        try {
            writeBytesMeter.mark(value.length);
            store.append(partition, key, value);
        } finally {
            context.stop();
        }

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
    public void purgeWriteCache() {
        final Timer.Context context = purgeTimer.time();
        try {
            store.purgeWriteCache();
        } finally {
            context.stop();
        }

    }

    @Override
    public Stream<byte[]> read(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.read(partition, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<byte[]> readSequential(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.readSequential(partition, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public byte[] readLast(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            byte[] bytes = store.readLast(partition, key);
            readBytesMeter.mark(bytes.length);
            return bytes;
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<byte[]> readFlushed(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.readFlushed(partition, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<byte[]> readSequentialFlushed(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            return store.readSequentialFlushed(partition, key)
                    .peek(bytes -> readBytesMeter.mark(bytes.length));
        } finally {
            context.stop();
        }
    }

    @Override
    public byte[] readLastFlushed(String partition, String key) {
        final Timer.Context context = readTimer.time();
        try {
            byte[] bytes = store.readLastFlushed(partition, key);
            readBytesMeter.mark(bytes.length);
            return bytes;
        } finally {
            context.stop();
        }
    }


    @Override
    public Stream<Map.Entry<String, Stream<byte[]>>> scan(String partition){
        final Timer.Context context = readTimer.time();
        try {
            return store
                    .scan(partition);
        } finally {
            context.stop();
        }
    }

    @Override
    public long size() {
        final Timer.Context context = sizeTimer.time();
        try {
            return store.size();
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> keys(String partition) {
        final Timer.Context context = keysTimer.time();
        try {
            return store.keys(partition);
        } finally {
            context.stop();
        }
    }

    @Override
    public Stream<String> partitions() {
        final Timer.Context context = partitionsTimer.time();
        try {
            return store.partitions();
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
    public void close() throws Exception {
        final Timer.Context context = closeTimer.time();
        try {
            store.close();
        } finally {
            context.stop();
        }
    }

    @Override
    public AppendStoreStats cacheStats(){
        return store.cacheStats();
    }

    @Override
    public String blockStats() {
        return store.blockStats();
    }

}
