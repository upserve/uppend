package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;

import java.nio.file.Path;

public class AppendOnlyStoreBuilder {
    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;
    private int flushDelaySeconds = FileAppendOnlyStore.DEFAULT_FLUSH_DELAY_SECONDS;
    private MetricRegistry metrics;

    public AppendOnlyStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public AppendOnlyStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.longLookupHashSize = longLookupHashSize;
        return this;
    }

    public AppendOnlyStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    public AppendOnlyStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    public AppendOnlyStoreBuilder withMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
        return this;
    }

    public AppendOnlyStore build() {
        AppendOnlyStore store = new FileAppendOnlyStore(dir, flushDelaySeconds, true, longLookupHashSize, longLookupWriteCacheSize);
        if (metrics != null) {
            store = new AppendOnlyStoreWithMetrics(store, metrics);
        }
        return store;
    }

    public ReadOnlyAppendOnlyStore buildReadOnly() {
        return new FileAppendOnlyStore(dir, -1, false, longLookupHashSize, 1);
    }

    @Override
    public String toString() {
        return "AppendOnlyStoreBuilder{" +
                "dir=" + dir +
                ", longLookupHashSize=" + longLookupHashSize +
                ", longLookupWriteCacheSize=" + longLookupWriteCacheSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                ", metrics=" + metrics +
                '}';
    }
}
