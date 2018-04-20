package com.upserve.uppend;

import com.upserve.uppend.metrics.CounterStoreWithMetrics;

public class CounterStoreBuilder extends FileStoreBuilder<CounterStoreBuilder> {

    public CounterStore build() {
        return build(false);
    }

    public CounterStore build(boolean readOnly) {
        if (readOnly && flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS) throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        CounterStore store = new FileCounterStore(readOnly, this);
        if (isStoreMetrics()) store = new CounterStoreWithMetrics(store, getStoreMetricsRegistry());
        return store;
    }

    public ReadOnlyCounterStore buildReadOnly() {
        return build(true);
    }

    private static CounterStoreBuilder defaultTestBuilder = new CounterStoreBuilder()
            .withInitialLookupKeyCacheSize(64)
            .withMaximumLookupKeyCacheWeight(100 * 1024)
            .withInitialMetaDataCacheSize(64)
            .withMaximumMetaDataCacheWeight(100 * 1024)
            .withIntialFileCacheSize(128)
            .withMaximumFileCacheSize(1024)
            .withLongLookupHashSize(16)
            .withLookupPageSize(16*1024)
            .withCacheMetrics();

    public static CounterStoreBuilder getDefaultTestBuilder(){
        return defaultTestBuilder;
    }
}
