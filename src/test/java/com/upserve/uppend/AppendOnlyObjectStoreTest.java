package com.upserve.uppend;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        instance.keys("partition");
        verify(store).keys("partition");
    }

    @Test
    public void testPartitions() {
        instance.partitions();
        verify(store).partitions();
    }

    @Test
    public void testClear() {
        instance.clear();
        verify(store).clear();
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

}
