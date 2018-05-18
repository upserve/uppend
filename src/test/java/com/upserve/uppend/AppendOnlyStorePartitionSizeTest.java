package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class AppendOnlyStorePartitionSizeTest {
    private final Path path = Paths.get("build/test/file-append-only-partition-size");

    private AppendOnlyStore newStore() {
        return AppendOnlyStoreBuilder.getDefaultTestBuilder().withDir(path.resolve("store-path")).withPartitionSize(2).build(false);
    }
    private AppendOnlyStore store;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void initialize() throws IOException {
        SafeDeleting.removeDirectory(path);
        store = newStore();
    }

    @After
    public void cleanUp() throws IOException {
        try {
            store.close();
        } catch (Exception e) {
            throw new AssertionError("Should not raise: {}", e);
        }
    }

    @Test
    public void testWithPartitionSize() {

        assertEquals(0, store.scan().count());
        assertEquals(0, store.keys().count());
        assertEquals(0, store.keyCount());
        assertEquals(0, store.read("foo", "bar").count());

        store.append("foo", "foobar", "bytes".getBytes());
        store.append("foo", "foobat", "bytes".getBytes());
        store.append("goo", "goobar", "bytes".getBytes());
        store.append("goo", "goobat", "bytes".getBytes());
        store.append("shoe", "shoebar", "bytes".getBytes());
        store.append("shoe", "shoebat", "bytes".getBytes());

        assertEquals(6, store.scan().count());
        assertEquals(0, store.keyCount());
        assertEquals(1, store.read("foo", "foobar").count());
        assertEquals(1, store.read("goo", "goobat").count());
        store.flush();
        assertEquals(6, store.keyCount());
    }
}
