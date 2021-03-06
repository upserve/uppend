package com.upserve.uppend;

import com.google.common.collect.ImmutableMap;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.*;

import static org.junit.Assert.*;

public class CounterStoreTest {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    Path path = Paths.get("build/test/file-counter-store");

    private CounterStore newStore() {
        return newStore(false);
    }
    private CounterStore newStore(boolean readOnly) {
        return TestHelper.getDefaultCounterStoreTestBuilder().withDir(path.resolve("store-path")).build(readOnly);
    }

    private CounterStore store;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void initialize() throws IOException {
        SafeDeleting.removeDirectory(path);
        store = newStore();
    }

    @After
    public void tearDown() {
        try {
            store.close();
        } catch (Exception e) {
            throw new AssertionError("Should not raise: {}", e);
        }
    }

    @Test
    public void testEmptyReadOnlyStore() throws Exception {
        tearDown();
        store = newStore(true);

        assertEquals(0, store.keyCount());
        assertEquals(0, store.keys().count());
        assertEquals(0, store.scan().count());
        assertNull(store.get("foo", "bar"));

        try  (CounterStore readWriteStore = newStore(false)){
            readWriteStore.increment("foo", "bar");
            readWriteStore.flush();

            assertEquals(1, store.keyCount());
            assertEquals(1, store.keys().count());
            assertEquals(1, store.scan().count());
            assertEquals(Long.valueOf(1), store.get("foo", "bar"));
        }
    }

    @Test
    public void setTest() throws Exception {
        store.set("partition", "foo", 5);
        assertEquals(Long.valueOf(5), store.get("partition", "foo"));
    }

    @Test
    public void incrTest() throws Exception {
        store.increment("partition", "foo", 1);
        assertEquals(Long.valueOf(1), store.get("partition", "foo"));
    }

    @Test
    public void incrTwiceTest() throws Exception {
        store.increment("partition", "foo", 1);
        store.increment("partition", "foo", 1);
        assertEquals(Long.valueOf(2), store.get("partition", "foo"));
    }

    @Test
    public void incrTwiceTwoTest() throws Exception {
        store.increment("partition", "foo", 1);
        store.increment("partition", "foo", 2);
        assertEquals(Long.valueOf(3), store.get("partition", "foo"));
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
        assertEquals(Long.valueOf(10), store.get("partition", "foo"));
        assertEquals(Long.valueOf(3), store.get("partition", "bar"));
        assertNull(store.get("partition", "baz"));
    }

    @Test
    public void testClear() {
        assertNull(store.set("partition", "foo", 7));
        store.clear();
        assertNull(store.get("partition", "foo"));
        assertEquals(0, store.keys().count());
        assertEquals(0, store.scan().count());
    }

    @Test
    public void testClearThenClose() throws Exception {
        store.clear();
        store.close();
    }

    @Test
    public void testPurge() throws Exception {
        assertNull(store.set("partition", "foo", 7));
        store.trim();
        assertEquals(Long.valueOf(7), store.get("partition", "foo"));
        assertEquals(8L, store.increment("partition", "foo"));
        assertEquals(Long.valueOf(8), store.get("partition", "foo"));
    }

    @Test
    public void testWriteThenClearThenClose() throws Exception {
        store.increment("partition", "foo");
        store.clear();
        store.close();
    }

    @Test
    public void testKeyCount() {
        assertEquals(0, store.keyCount());
        store.increment("foo", "bar");
        store.increment("foo", "bar");
        store.increment("fooTwo", "bar");
        store.increment("fooTwo", "barOne");
        assertEquals(0, store.keyCount());
        store.flush();
        assertEquals(3, store.keyCount());
        store.clear();
        assertEquals(0, store.keyCount());
    }

    @Test
    public void testScan() {
        store.increment("partition_one", "one", 1);
        store.increment("partition_one", "two", 1);
        store.increment("partition_one", "three", 1);
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "five", 1);

        Map<String, Long> result = store
                .scan()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        Map<String, Long> expected = ImmutableMap.of(
                "one", 2L,
                "two", 1L,
                "three", 1L,
                "five", 1L
        );

        assertEquals(expected, result);
    }

    @Test
    public void testScanCallback() {
        store.increment("partition_one", "one", 1);
        store.increment("partition_one", "two", 1);
        store.increment("partition_one", "three", 1);
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "five", 1);

        ConcurrentMap<String, Long> result = new ConcurrentHashMap<>();
        store.scan(result::put);

        Map<String, Long> expected = ImmutableMap.of(
                "one", 2L,
                "two", 1L,
                "three", 1L,
                "five", 1L
        );

        assertEquals(expected, result);
    }

    @Test
    public void testExample() {
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "ccccccc-cccccccccc-ccccccc-ccccccc::ccccccc");
        store.increment("2017-11-30", "ttt-ttttt-tttt-ttttttt-ttt-tttt::tttttttttt");

        assertEquals(Long.valueOf(5), store.get("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb"));
        assertEquals(Long.valueOf(1), store.get("2017-11-30", "ccccccc-cccccccccc-ccccccc-ccccccc::ccccccc"));
        assertEquals(Long.valueOf(1), store.get("2017-11-30", "ttt-ttttt-tttt-ttttttt-ttt-tttt::tttttttttt"));
    }

    @Test
    public void testParallelWriteThenRead() throws Exception {
        int keys = 512;
        LongAdder[] counts = IntStream.range(0, keys).mapToObj(v -> new LongAdder()).toArray(LongAdder[]::new);
        new Random()
                .ints(500_000, 0, keys)
                .parallel()
                .forEach(i -> {
                    store.increment(String.format("p%03d", (i % 5)), String.format("k%04d" , i));
                    counts[i].increment();
                });
        IntStream.range(0, keys).forEach(i -> {
            assertEquals(
                    counts[i].longValue() > 0 ? counts[i].longValue() : null,
                    store.get(String.format("p%03d", (i % 5)), String.format("k%04d" , i))
            );
        });
    }
}
