package com.upserve.uppend.metrics;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AppendOnlyStoreWithMetricsTest {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Mock
    private AppendOnlyStore store;

    private MetricRegistry metrics;

    private AppendOnlyStoreWithMetrics instance;

    @Before
    public void before() {
        metrics = new MetricRegistry();
        when(store.getName()).thenReturn("testStore");
        instance = new AppendOnlyStoreWithMetrics(store, metrics, "MetricsRoot");
    }

    @Test
    public void testAppend() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), WRITE_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(),  WRITE_BYTES_METER_METRIC_NAME)).getCount());
        byte[] val = new byte[]{0x01, 0x02, 0x03};
        instance.append("partition", "key", val);
        verify(store).append("partition", "key", val);
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), WRITE_TIMER_METRIC_NAME)).getCount());
        assertEquals(3, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), WRITE_BYTES_METER_METRIC_NAME)).getCount());
    }

    @Test
    public void testRead() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
        when(store.read("partition", "key1"))
                .thenReturn(Stream.of(
                        "first".getBytes(),
                        "second".getBytes()
                ));
        assertArrayEquals(
                Arrays.asList("first", "second").toArray(),
                instance.read("partition", "key1").map(String::new).toArray()
        );
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(11, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
    }

    @Test
    public void testReadSequential() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
        when(store.readSequential("partition", "key1"))
                .thenReturn(Stream.of(
                        "first".getBytes(),
                        "second".getBytes()
                ));
        assertArrayEquals(
                Arrays.asList("first", "second").toArray(),
                instance.readSequential("partition", "key1").map(String::new).toArray()
        );
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(11, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
    }

    @Test
    public void testReadLast() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
        when(store.readLast("partition", "key1"))
                .thenReturn("last".getBytes());
        assertEquals(
                "last",
                new String(instance.readLast("partition", "key1"))
        );
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_TIMER_METRIC_NAME)).getCount());
        assertEquals(4, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), READ_BYTES_METER_METRIC_NAME)).getCount());
    }

    @Test
    public void testClear() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME)).getCount());
        instance.clear();
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), CLEAR_TIMER_METRIC_NAME)).getCount());
    }

    @Test
    public void testScanStream() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_BYTES_METER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_KEYS_METER_METRIC_NAME)).getCount());
        when(store.scan())
                .thenReturn(Map.of(
                        "key1", Stream.of("val1.1".getBytes(), "val1.2".getBytes()),
                        "key2", Stream.of("val2.1".getBytes())
                ).entrySet().stream());
        instance.scan().forEach(entry -> {
            String key = entry.getKey();
            entry.getValue().forEach(val -> log.trace("scanned: key={}, val={}", key, new String(val)));
        });
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
        assertEquals(18, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_BYTES_METER_METRIC_NAME)).getCount());
        assertEquals(2, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_KEYS_METER_METRIC_NAME)).getCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanCallback() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_BYTES_METER_METRIC_NAME)).getCount());
        assertEquals(0, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_KEYS_METER_METRIC_NAME)).getCount());

        BiConsumer<String, Stream<byte[]>> callback = (key, vals) -> {
            vals.forEach(val -> log.trace("scanned: key={}, val={}", key, new String(val)));
        };

        doAnswer((Answer<Void>) invocation -> {
            BiConsumer<String, Stream<byte[]>> _callback = invocation.getArgument(0);
            _callback.accept("key1", Stream.of("val1.1".getBytes(), "val1.2".getBytes()));
            _callback.accept("key2", Stream.of("val2.1".getBytes()));
            return null;
        }).when(store).scan(any(BiConsumer.class));

        instance.scan(callback);

        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_TIMER_METRIC_NAME)).getCount());
        assertEquals(18, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_BYTES_METER_METRIC_NAME)).getCount());
        assertEquals(2, metrics.meter(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), SCAN_KEYS_METER_METRIC_NAME)).getCount());
    }

    @Test
    public void testKeys() {
        assertEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), KEYS_TIMER_METRIC_NAME)).getCount());
        when(store.keys()).thenReturn(Stream.of("key1", "key2", "key3", "key4", "key5"));
        assertEquals(5, instance.keys().count());
        assertNotEquals(0, metrics.timer(MetricRegistry.name("MetricsRoot", UPPEND_APPEND_STORE, store.getName(), KEYS_TIMER_METRIC_NAME)).getCount());
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
    public void testGetBlockLongStats() {
        BlockStats v = new BlockStats(0, 0, 0, 0, 0);
        when(store.getBlockLongStats()).thenReturn(v);
        assertEquals(v, instance.getBlockLongStats());
    }

    @Test
    public void testKeyCount() {
        when(store.keyCount()).thenReturn(5L);
        assertEquals(5, instance.keyCount());
    }
}
