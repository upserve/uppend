package com.upserve.uppend;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AppendOnlyObjectStoreTest {

    static class Data {
        static byte[] serialize(Data d) {
            if (d == DESERIALIZED) {
                return SERIALIZED;
            } else {
                return null;
            }
        }

        static Data deserialize(byte[] d) {
            if (d == SERIALIZED) {
                return DESERIALIZED;
            } else {
                return null;
            }
        }
    }
    static final byte[] SERIALIZED = "SERIALIZED".getBytes();
    static final Data DESERIALIZED = new Data();

    @Mock
    AppendOnlyStore store;

    AppendOnlyObjectStore<Data> instance;

    @Before
    public void before() {
        instance = new AppendOnlyObjectStore<>(store, Data::serialize, Data::deserialize);
    }

    @Test
    public void testAppend() {
        instance.append("partition", "key", DESERIALIZED);
        verify(store).append("partition", "key", SERIALIZED);
    }

    @Test
    public void testRead() {
        when(store.read("partition", "key1"))
                .thenReturn(Arrays.asList(SERIALIZED).stream());
        assertArrayEquals(
                instance.read("partition", "key1").toArray(),
                Arrays.asList(DESERIALIZED).toArray()
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
