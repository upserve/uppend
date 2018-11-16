package com.upserve.uppend.metrics;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.*;
import com.upserve.uppend.lookup.FlushStats;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

import static com.upserve.uppend.metrics.CounterStoreWithMetrics.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CounterStoreWithMetricsTest {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Mock
    private CounterStore store;

    private MetricRegistry metrics;

    private CounterStoreWithMetrics instance;

    @Before
    public void before() {
        metrics = new MetricRegistry();
        when(store.getName()).thenReturn("testStore");
        instance = new CounterStoreWithMetrics(store, metrics, "MetricsRoot");
    }

    @Test
    public void testGetName() {
        assertEquals("testStore", instance.getName());
    }


    @Test
    public void testIncrement() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), INCREMENT_TIMER_METRIC_NAME)).getCount());
        instance.increment("partition", "key");
        verify(store).increment("partition", "key", 1);
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), INCREMENT_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testIncrementDelta() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), INCREMENT_TIMER_METRIC_NAME)).getCount());
        instance.increment("partition", "key", 3);
        verify(store).increment("partition", "key", 3);
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), INCREMENT_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testSet() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SET_TIMER_METRIC_NAME)).getCount());
        instance.set("partition", "key", 4);
        verify(store).set("partition", "key", 4);
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SET_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testRead() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), GET_TIMER_METRIC_NAME)).getCount());
        when(store.get("partition", "key1")).thenReturn(12L);
        assertEquals(12, instance.get("partition", "key1").longValue());
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), GET_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testClear() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME)).getCount());
        instance.clear();
        verify(store).clear();
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testScanStream() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
        when(store.scan())
                .thenReturn(Map.of(
                        "key1", 1L,
                        "key2", 2L
                ).entrySet().stream());
        instance.scan().forEach(entry -> {
            String key = entry.getKey();
            long val = entry.getValue();
            log.trace("scanned: key={}, val={}", key, val);
        });
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanCallback() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());

        ObjLongConsumer<String> callback = (key, val) -> log.trace("scanned: key={}, val={}", key, val);

        doAnswer((Answer<Void>) invocation -> {
            ObjLongConsumer<String> _callback = invocation.getArgument(0);
            _callback.accept("key1", 1);
            _callback.accept("key2", 2);
            return null;
        }).when(store).scan(any(ObjLongConsumer.class));

        instance.scan(callback);

        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testKeys() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), KEYS_TIMER_METRIC_NAME)).getCount());
        when(store.keys()).thenReturn(Stream.of("key1", "key2", "key3", "key4", "key5"));
        assertEquals(5, instance.keys().count());
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), KEYS_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testClose() throws Exception {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), CLOSE_TIMER_METRIC_NAME)).getCount());
        instance.close();
        verify(store).close();
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), CLOSE_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testTrim() throws Exception {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), TRIM_TIMER_METRIC_NAME)).getCount());
        instance.trim();
        verify(store).trim();
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_COUNTER_STORE, store.getName(), TRIM_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testRegister() {
        instance.register(10);
        verify(store).register(10);
    }

    @Test
    public void testDeregister() {
        instance.deregister();
        verify(store).deregister();
    }

    @Test
    public void testKeyCount() {
        when(store.keyCount()).thenReturn(5L);
        assertEquals(5, instance.keyCount());
    }
}
