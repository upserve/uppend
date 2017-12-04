package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.metrics.CounterStoreWithMetrics;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

public class CounterStoreBuilder {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;
    private int flushDelaySeconds = FileCounterStore.DEFAULT_FLUSH_DELAY_SECONDS;
    private MetricRegistry metrics;

    public CounterStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public CounterStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.longLookupHashSize = longLookupHashSize;
        return this;
    }

    public CounterStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    public CounterStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    public CounterStoreBuilder withMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
        return this;
    }

    public CounterStore build() {
        CounterStore store = new FileCounterStore(dir, flushDelaySeconds, true, longLookupHashSize, longLookupWriteCacheSize);
        if (metrics != null) {
            store = new CounterStoreWithMetrics(store, metrics);
        }
        return store;
    }

    public ReadOnlyCounterStore buildReadOnly() {
        return new FileCounterStore(dir, -1, false, longLookupHashSize, 1);
    }

    @Override
    public String toString() {
        return "CounterStoreBuilder{" +
                "dir=" + dir +
                ", longLookupHashSize=" + longLookupHashSize +
                ", longLookupWriteCacheSize=" + longLookupWriteCacheSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                '}';
    }
}
