package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;

import static org.junit.Assert.*;

public class UppendTest {
    @Test
    public void testVersion() {
        // NOTE: source of this value is src/test/resources/com/upserve/uppend/main.properties
        assertEquals("test-version", Uppend.VERSION);
    }

    @Test
    public void testStore() throws Exception {
        final String pathStr = "build/tmp/test/uppend-test-store/";
        final Path path = Paths.get(pathStr);
        SafeDeleting.removeTempPath(path);
        assertFalse(Files.exists(path));
        AppendOnlyStore store = Uppend.store(pathStr).build();
        store.append("partition", "foo", "bar".getBytes());
        assertTrue(Files.exists(path));
        store.flush();
        ReadOnlyAppendOnlyStore store2 = Uppend.store(path).buildReadOnly();
        assertArrayEquals(new String[] { "bar" }, store2.read("partition", "foo").map(String::new).toArray());
        store.close();
        store2.close();
        SafeDeleting.removeTempPath(path);
    }

    @Test
    public void testCounterStore() throws Exception {
        final String pathStr = "build/tmp/test/uppend-test-counter-store/";
        final Path path = Paths.get(pathStr);
        SafeDeleting.removeTempPath(path);
        assertFalse(Files.exists(path));
        CounterStore store = Uppend.counterStore(pathStr).build();
        store.increment("partition", "foo", 5);
        assertTrue(Files.exists(path));
        store.flush();
        ReadOnlyCounterStore store2 = Uppend.counterStore(path).buildReadOnly();
        assertEquals(5, store2.get("partition", "foo"));
        store.close();
        store2.close();
        SafeDeleting.removeTempPath(path);
    }
}
