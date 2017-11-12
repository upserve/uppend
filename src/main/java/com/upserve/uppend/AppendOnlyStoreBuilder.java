package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;

public interface AppendOnlyStoreBuilder<T extends AppendOnlyStore> {
    T build();

    default AppendOnlyStore buildWithMetrics(MetricRegistry metrics) {
        T store = build();
        return new AppendOnlyStoreWithMetrics(store, metrics);
    }
}
