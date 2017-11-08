package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.file.*;

import static org.junit.Assert.*;

@Slf4j
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
        FileAppendOnlyStore store = Uppend.store(pathStr).build();
        store.append("partition", "foo", "bar".getBytes());
        assertTrue(Files.exists(path));
        store.flush();
        FileAppendOnlyStore store2 = Uppend.store(path).build();
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
        FileCounterStore store = Uppend.counterStore(pathStr).build();
        store.increment("partition", "foo", 5);
        assertTrue(Files.exists(path));
        store.flush();
        FileCounterStore store2 = Uppend.counterStore(path).build();
        assertEquals(5, store2.get("partition", "foo"));
        store.close();
        store2.close();
        SafeDeleting.removeTempPath(path);
    }
}
