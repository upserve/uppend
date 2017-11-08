package com.upserve.uppend;

import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public abstract class CounterStoreTest {
    protected abstract CounterStore newStore();

    private CounterStore store;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void initialize() {
        store = newStore();
        store.clear();
    }

    @After
    public void cleanUp(){
        try {
            store.close();
        } catch (Exception e){
            throw new AssertionError("Should not raise: {}", e);
        }
    }

    @Test
    public void testWriteCloseReadRepeat() throws Exception {
        store.set("partition", "foo", 5);
        store.increment("partition", "foo");
        store.increment("partition", "foo", 2);
        store.close();
        store = newStore();
        store.increment("partition", "foo", 2);
        store.increment("partition", "bar", 3);
        store.close();
        store = newStore();
        assertEquals(9, store.get("partition", "foo"));
        assertEquals(3, store.get("partition", "bar"));
        assertEquals(0, store.get("partition", "baz"));
    }

    @Test
    public void testClear() throws Exception {
        store.set("partition", "foo", 7);
        store.clear();
        assertEquals(0, store.get("partition", "foo"));
        assertEquals(0, store.partitions().count());
        assertEquals(0, store.keys("partition").count());
    }

    @Test
    public void testPartitions() throws Exception {
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "two", 2);
        store.increment("partition$three", "three", 3);
        store.increment("partition-four", "four", 4);
        store.increment("_2016-01-02", "five", 5);
        assertArrayEquals(new String[] { "_2016-01-02", "partition$three", "partition-four",  "partition_one", "partition_two" }, store.partitions().sorted().toArray(String[]::new));
    }
}
