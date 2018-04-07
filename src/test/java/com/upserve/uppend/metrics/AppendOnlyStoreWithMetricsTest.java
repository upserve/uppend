package com.upserve.uppend.metrics;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.AppendOnlyStore;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AppendOnlyStoreWithMetricsTest {
    @Mock
    private AppendOnlyStore store;

    private MetricRegistry metrics;

    private AppendOnlyStoreWithMetrics instance;

    @Before
    public void before() {
        metrics = new MetricRegistry();
        instance = new AppendOnlyStoreWithMetrics(store, metrics);
    }

    @Test
    public void testAppend() {
        assertEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.WRITE_TIMER_METRIC_NAME).getCount());
        assertEquals(0, metrics.meter(AppendOnlyStoreWithMetrics.WRITE_BYTES_METER_METRIC_NAME).getCount());
        byte[] val = new byte[] { 0x01, 0x02, 0x03 };
        instance.append("partition", "key", val);
        verify(store).append("partition", "key", val);
        assertNotEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.WRITE_TIMER_METRIC_NAME).getCount());
        assertEquals(3, metrics.meter(AppendOnlyStoreWithMetrics.WRITE_BYTES_METER_METRIC_NAME).getCount());
    }

    @Test
    public void testRead() {
        assertEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(0, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
        when(store.read("partition", "key1"))
                .thenReturn(Stream.of(
                        "first".getBytes(),
                        "second".getBytes()
                ));
        assertArrayEquals(
                Arrays.asList("first", "second").toArray(),
                instance.read("partition", "key1").map(String::new).toArray()
        );
        assertNotEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(11, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
    }

    @Test
    public void testReadSequential() {
        assertEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(0, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
        when(store.readSequential("partition", "key1"))
                .thenReturn(Stream.of(
                        "first".getBytes(),
                        "second".getBytes()
                ));
        assertArrayEquals(
                Arrays.asList("first", "second").toArray(),
                instance.readSequential("partition", "key1").map(String::new).toArray()
        );
        assertNotEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(11, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
    }

    @Test
    public void testReadLast() {
        assertEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(0, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
        when(store.readLast("partition", "key1"))
                .thenReturn("last".getBytes());
        assertEquals(
                "last",
                new String(instance.readLast("partition", "key1"))
        );
        assertNotEquals(0, metrics.timer(AppendOnlyStoreWithMetrics.READ_TIMER_METRIC_NAME).getCount());
        assertEquals(4, metrics.meter(AppendOnlyStoreWithMetrics.READ_BYTES_METER_METRIC_NAME).getCount());
    }
}
