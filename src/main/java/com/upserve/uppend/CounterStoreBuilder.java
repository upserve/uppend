package com.upserve.uppend;

import com.upserve.uppend.metrics.CounterStoreWithMetrics;

public class CounterStoreBuilder extends FileStoreBuilder<CounterStoreBuilder> {

    public CounterStore build() {
        return build(false);
    }

    public CounterStore build(boolean readOnly) {
        if (readOnly && flushDelaySeconds != DEFAULT_FLUSH_DELAY_SECONDS)
            throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        CounterStore store = new FileCounterStore(readOnly, this);
        if (isStoreMetrics()) store = new CounterStoreWithMetrics(store, getStoreMetricsRegistry(), getMetricsRootName());
        return store;
    }

    public ReadOnlyCounterStore buildReadOnly() {
        return build(true);
    }

    private static CounterStoreBuilder defaultTestBuilder = new CounterStoreBuilder()
            .withStoreName("test")
            .withInitialLookupKeyCacheSize(64)
            .withMaximumLookupKeyCacheWeight(100 * 1024)
            .withInitialMetaDataCacheSize(64)
            .withMaximumMetaDataCacheWeight(100 * 1024)
            .withMetaDataPageSize(1024)
            .withLongLookupHashSize(16)
            .withLookupPageSize(16 * 1024)
            .withCacheMetrics();

    public static CounterStoreBuilder getDefaultTestBuilder() {
        return defaultTestBuilder;
    }

    @Override
    public String toString() {
        return "CounterStoreBuilder{" +
                "storeName='" + storeName + '\'' +
                ", partitionSize=" + partitionSize +
                ", lookupHashSize=" + lookupHashSize +
                ", lookupPageSize=" + lookupPageSize +
                ", initialLookupPageCacheSize=" + initialLookupPageCacheSize +
                ", maximumLookupPageCacheSize=" + maximumLookupPageCacheSize +
                ", maximumLookupKeyCacheWeight=" + maximumLookupKeyCacheWeight +
                ", initialLookupKeyCacheSize=" + initialLookupKeyCacheSize +
                ", maximumMetaDataCacheWeight=" + maximumMetaDataCacheWeight +
                ", initialMetaDataCacheSize=" + initialMetaDataCacheSize +
                ", metadataTTL=" + metadataTTL +
                ", metaDataPageSize=" + metaDataPageSize +
                ", lookupKeyCacheExecutorService=" + lookupKeyCacheExecutorService +
                ", lookupMetaDataCacheExecutorService=" + lookupMetaDataCacheExecutorService +
                ", lookupPageCacheExecutorService=" + lookupPageCacheExecutorService +
                ", flushDelaySeconds=" + flushDelaySeconds +
                ", flushThreshold=" + flushThreshold +
                ", dir=" + dir +
                ", storeMetricsRegistry=" + storeMetricsRegistry +
                ", metricsRootName='" + metricsRootName + '\'' +
                ", storeMetrics=" + storeMetrics +
                ", cacheMetricsRegistry=" + cacheMetricsRegistry +
                ", cacheMetrics=" + cacheMetrics +
                '}';
    }
}
