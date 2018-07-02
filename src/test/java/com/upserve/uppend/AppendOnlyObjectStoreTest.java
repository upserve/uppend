package com.upserve.uppend;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AppendOnlyObjectStoreTest {
    static class Data {
        private final String value;

        public Data(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Data data = (Data) o;

            return value != null ? value.equals(data.value) : data.value == null;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        static byte[] serialize(Data d) {
            return d.value.getBytes();
        }

        static Data deserialize(byte[] d) {
            return new Data(new String(d));
        }
    }

    static final byte[] SERIALIZED01 = "SERIALIZED01".getBytes();
    static final Data DESERIALIZED01 = new Data("SERIALIZED01");

    static final byte[] SERIALIZED02 = "SERIALIZED02".getBytes();
    static final Data DESERIALIZED02 = new Data("SERIALIZED02");

    @Mock
    AppendOnlyStore store;

    AppendOnlyObjectStore<Data> instance;

    @Before
    public void before() {
        instance = new AppendOnlyObjectStore<>(store, Data::serialize, Data::deserialize);
    }

    @Test
    public void testAppend() {
        instance.append("partition", "key", DESERIALIZED01);
        verify(store).append("partition", "key", SERIALIZED01);
    }

    @Test
    public void testRead() {
        when(store.read("partition", "key1"))
                .thenReturn(Stream.of(SERIALIZED02, SERIALIZED01));
        assertArrayEquals(
                Arrays.asList(DESERIALIZED02, DESERIALIZED01).toArray(),
                instance.read("partition", "key1").toArray()
        );
    }

    @Test
    public void testReadSequential() {
        when(store.readSequential("partition", "key1"))
                .thenReturn(Stream.of(SERIALIZED01, SERIALIZED02));
        assertArrayEquals(
                Arrays.asList(DESERIALIZED01, DESERIALIZED02).toArray(),
                instance.readSequential("partition", "key1").toArray()
        );
    }

    @Test
    public void testReadLast() {
        when(store.readLast("partition", "key1"))
                .thenReturn(SERIALIZED02);
        assertEquals(
                DESERIALIZED02,
                instance.readLast("partition", "key1")
        );
    }

    @Test
    public void testKeys() {
        instance.keys();
        verify(store).keys();
    }

    @Test
    public void testClear() {
        instance.clear();
        verify(store).clear();
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
    public void testTrim() {
        instance.trim();
        verify(store).trim();
    }

    @Test
    public void testGetName() {
        when(store.getName()).thenReturn("my-name");
        assertEquals("my-name", instance.getName());
    }

    @Test
    public void testGetStore() {
        assertEquals(store, instance.getStore());
    }

    @Test
    public void testFlush() throws Exception {
        instance.flush();
        verify(store).flush();
    }

    @Test
    public void testClose() throws Exception {
        instance.close();
        verify(store).close();
    }

    @Test
    public void testKeyCount() {
        when(store.keyCount()).thenReturn(7L);
        assertEquals(7, instance.keyCount());
    }

    @Test
    public void testScanStream() {
        Map<String, List<String>> results = new HashMap<>();
        when(store.scan())
                .thenReturn(Map.of(
                        "key1", Stream.of("val1.1".getBytes(), "val1.2".getBytes()),
                        "key2", Stream.of("val2.1".getBytes())
                ).entrySet().stream());
        instance.scan().forEach(entry -> {
            String key = entry.getKey();
            results.computeIfAbsent(key, k -> new ArrayList<>());
            entry.getValue().forEach(val -> results.get(key).add(val.value));
        });
        assertEquals(2, results.size());
        assertArrayEquals(new String[] { "key1", "key2" }, results.keySet().stream().sorted().toArray());
        assertArrayEquals(new String[] { "val1.1", "val1.2" }, results.get("key1").toArray());
        assertArrayEquals(new String[] { "val2.1" }, results.get("key2").toArray());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testScanCallback() {
        Map<String, List<String>> results = new HashMap<>();
        BiConsumer<String, Stream<Data>> callback = (key, vals) -> {
            results.computeIfAbsent(key, k -> new ArrayList<>());
            vals.forEach(val -> results.get(key).add(val.value));
        };
        //noinspection Duplicates
        doAnswer((Answer<Void>) invocation -> {
            BiConsumer<String, Stream<byte[]>> _callback = invocation.getArgument(0);
            _callback.accept("key1", Stream.of("val1.1".getBytes(), "val1.2".getBytes()));
            _callback.accept("key2", Stream.of("val2.1".getBytes()));
            return null;
        }).when(store).scan(any(BiConsumer.class));
        instance.scan(callback);
        assertEquals(2, results.size());
        assertArrayEquals(new String[] { "key1", "key2" }, results.keySet().stream().sorted().toArray());
        assertArrayEquals(new String[] { "val1.1", "val1.2" }, results.get("key1").toArray());
        assertArrayEquals(new String[] { "val2.1" }, results.get("key2").toArray());
    }
}
