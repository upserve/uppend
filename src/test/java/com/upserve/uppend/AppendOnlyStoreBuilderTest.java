package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;

import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.FLUSH_TIMER_METRIC_NAME;
import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.UPPEND_APPEND_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AppendOnlyStoreBuilderTest {
    @Test
    public void testBuild() throws Exception {
        Path path = Paths.get("build/tmp/test/append-only-store-builder");
        SafeDeleting.removeDirectory(path);
        Uppend.store(path).build();
        assertTrue(Files.exists(path));
    }

    @Test
    public void testBuildWithMetrics() throws Exception {
        Path path = Paths.get("build/tmp/test/append-only-store-builder");
        SafeDeleting.removeDirectory(path);
        MetricRegistry metrics = new MetricRegistry();
        AppendOnlyStore store = Uppend.store(path).withStoreMetrics(metrics).withMetricsRootName("Root").withMetricsInstanceID("default").build(false);
        store.flush();
        assertEquals(1, metrics.getTimers().get(MetricRegistry.name("Root", UPPEND_APPEND_STORE, store.getName(), FLUSH_TIMER_METRIC_NAME)).getCount());
    }
}
