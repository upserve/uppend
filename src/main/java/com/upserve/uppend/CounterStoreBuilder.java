package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.metrics.CounterStoreWithMetrics;

public interface CounterStoreBuilder<T extends CounterStore> {
    T build();

    default CounterStore buildWithMetrics(MetricRegistry metrics) {
        T store = build();
        return new CounterStoreWithMetrics(store, metrics);
    }
}
