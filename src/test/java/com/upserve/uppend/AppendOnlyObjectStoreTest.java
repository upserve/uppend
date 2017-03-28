package com.upserve.uppend;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        instance.append("prefix", "key", DESERIALIZED);
        verify(store).append("prefix", "key", SERIALIZED);
    }

    @Test
    public void testRead() {
        when(store.read("prefix", "key1"))
                .thenReturn(Arrays.asList(SERIALIZED).stream());
        assertArrayEquals(
                instance.read("prefix", "key1").toArray(),
                Arrays.asList(DESERIALIZED).toArray()
        );
    }

    @Test
    public void testKeys() {
        instance.keys("prefix");
        verify(store).keys("prefix");
    }

    @Test
    public void testPrefixes() {
        instance.prefixes();
        verify(store).prefixes();
    }

    @Test
    public void testClear() {
        instance.clear();
        verify(store).clear();
    }

    @Test
    public void testClose() throws Exception {
        instance.close();
        verify(store).close();
    }

}
