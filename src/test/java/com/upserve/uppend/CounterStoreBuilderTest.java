package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;

import static org.junit.Assert.assertEquals;

public class CounterStoreBuilderTest {
    @Test
    public void testBuildWithMetrics() throws Exception {
        Path path = Paths.get("build/tmp/test/counter-store-builder");
        SafeDeleting.removeDirectory(path);
        MetricRegistry metrics = new MetricRegistry();
        CounterStore store = Uppend.counterStore(path).withMetrics(metrics).build();
        store.flush();
        assertEquals(1, metrics.getTimers().get("flush").getCount());
    }
}
