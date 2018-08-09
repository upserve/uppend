package com.upserve.uppend;

import com.upserve.uppend.metrics.CounterStoreWithMetrics;

public class CounterStoreBuilder extends FileStoreBuilder<CounterStoreBuilder> {
    public CounterStore build() {
        return build(false);
    }

    public CounterStore build(boolean readOnly) {
        if (readOnly && getFlushDelaySeconds() != DEFAULT_FLUSH_DELAY_SECONDS)
            throw new IllegalStateException("Can not set flush delay seconds in read only mode");
        CounterStore store = new FileCounterStore(readOnly, this);
        if (isStoreMetrics()) store = new CounterStoreWithMetrics(store, getStoreMetricsRegistry(), getMetricsRootName());
        return store;
    }

    public ReadOnlyCounterStore buildReadOnly() {
        return build(true);
    }

    @Override
    public String toString() {
        return "CounterStoreBuilder{}" + super.toString();
    }
}
