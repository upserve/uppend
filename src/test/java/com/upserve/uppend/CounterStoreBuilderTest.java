package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;

import static com.upserve.uppend.metrics.CounterStoreWithMetrics.FLUSH_TIMER_METRIC_NAME;
import static com.upserve.uppend.metrics.CounterStoreWithMetrics.UPPEND_COUNTER_STORE;
import static org.junit.Assert.assertEquals;

public class CounterStoreBuilderTest {
    @Test
    public void testBuildWithMetrics() throws Exception {
        Path path = Paths.get("build/tmp/test/counter-store-builder");
        SafeDeleting.removeDirectory(path);
        MetricRegistry metrics = new MetricRegistry();
        CounterStore store = Uppend.counterStore(path).withStoreMetrics(metrics).withStoreName("testStore").withMetricsRootName("Root").build();
        store.flush();
        assertEquals(1, metrics.getTimers().get(MetricRegistry.name("Root", UPPEND_COUNTER_STORE, "testStore", FLUSH_TIMER_METRIC_NAME)).getCount());
    }
}
