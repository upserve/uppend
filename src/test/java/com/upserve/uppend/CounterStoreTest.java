package com.upserve.uppend;

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class CounterStoreTest {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private CounterStore newStore() {
        return new CounterStoreBuilder().withDir(Paths.get("build/test/file-append-only-store")).build();
    }

    private CounterStore store;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void initialize() {
        store = newStore();
        store.clear();
    }

    @After
    public void cleanUp() {
        try {
            store.close();
        } catch (Exception e) {
            throw new AssertionError("Should not raise: {}", e);
        }
    }

    @Test
    public void setTest() throws Exception {
        store.set("partition", "foo", 5);
        assertEquals(5, store.get("partition", "foo"));
    }

    @Test
    public void incrTest() throws Exception {
        store.increment("partition", "foo", 1);
        assertEquals(1, store.get("partition", "foo"));
    }

    @Test
    public void incrTwiceTest() throws Exception {
        store.increment("partition", "foo", 1);
        store.increment("partition", "foo", 1);
        assertEquals(2, store.get("partition", "foo"));
    }

    @Test
    public void incrTwiceTwoTest() throws Exception {
        store.increment("partition", "foo", 1);
        store.increment("partition", "foo", 2);
        assertEquals(3, store.get("partition", "foo"));
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
        assertEquals(10, store.get("partition", "foo"));
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
    public void testClearThenClose() throws Exception {
        store.clear();
        store.close();
    }

    @Test
    public void testWriteThenClearThenClose() throws Exception {
        store.increment("partition", "foo");
        store.clear();
        store.close();
    }

    @Test
    public void testPartitions() throws Exception {
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "two", 2);
        store.increment("partition$three", "three", 3);
        store.increment("partition-four", "four", 4);
        store.increment("_2016-01-02", "five", 5);
        assertArrayEquals(new String[] { "_2016-01-02", "partition$three", "partition-four", "partition_one", "partition_two" }, store.partitions().sorted().toArray(String[]::new));
    }

    @Test
    public void testScan() {
        store.increment("partition_one", "one", 1);
        store.increment("partition_one", "two", 1);
        store.increment("partition_one", "three", 1);
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "one", 1);

        Map<String, Long> result = store
                .scan("partition_one")
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        Map<String, Long> expected = ImmutableMap.of(
                "one", 2L,
                "two", 1L,
                "three", 1L
        );

        assertEquals(expected, result);
    }

    @Test
    public void testScanCallback() {
        store.increment("partition_one", "one", 1);
        store.increment("partition_one", "two", 1);
        store.increment("partition_one", "three", 1);
        store.increment("partition_one", "one", 1);
        store.increment("partition_two", "one", 1);

        Map<String, Long> result = new TreeMap<>();
        store.scan("partition_one", result::put);

        Map<String, Long> expected = ImmutableMap.of(
                "one", 2L,
                "two", 1L,
                "three", 1L
        );

        assertEquals(expected, result);
    }

    @Test
    public void testExample() throws Exception {
        store.close();
        store = new FileCounterStore(Paths.get("build/test/file-append-only-store"), 10, true, 1, 1);
        store.clear();

        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");
        store.increment("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb");

        store.increment("2017-11-30", "ccccccc-cccccccccc-ccccccc-ccccccc::ccccccc");

        store.increment("2017-11-30", "ttt-ttttt-tttt-ttttttt-ttt-tttt::tttttttttt");

        assertArrayEquals(new String[] { "2017-11-30" }, store.partitions().toArray(String[]::new));
        assertEquals(5, store.get("2017-11-30", "bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb"));
        assertEquals(1, store.get("2017-11-30", "ccccccc-cccccccccc-ccccccc-ccccccc::ccccccc"));
        assertEquals(1, store.get("2017-11-30", "ttt-ttttt-tttt-ttttttt-ttt-tttt::tttttttttt"));
    }

    @Test
    public void testParallel() throws Exception {
        store.close();
        store = new FileCounterStore(Paths.get("build/test/file-append-only-store"), 10, true, 1, 1);
        store.clear();

        final int numKeys = 1000;
        final int totalIncrements = 1_000_000;
        log.info("parallel: starting {} keys, {} total increments", numKeys, totalIncrements);
        long[] vals = new long[numKeys];
        ArrayList<Runnable> jobs = new ArrayList<>();
        Random rand = new Random();
        log.info("parallel: creating jobs");
        for (int i = 0; i < totalIncrements; i++) {
            int keyNum = rand.nextInt(numKeys);
            vals[keyNum]++;
            String key = String.format("k%010d", keyNum);
            jobs.add(() -> store.increment("my_partition", key));
        }
        Collections.shuffle(jobs);
        ArrayList<ForkJoinTask> futures = new ArrayList<>();
        log.info("parallel: submitting jobs");
        jobs.forEach(job -> futures.add(ForkJoinPool.commonPool().submit(job)));
        log.info("parallel: waiting for jobs");
        futures.forEach(ForkJoinTask::join);

        log.info("parallel: flushing");
        store.flush();

        log.info("parallel: comparing");
        for (int i = 0; i < vals.length; i++) {
            long val = vals[i];
            String key = String.format("k%010d", i);
            assertEquals("expected value " + (i + 1) + "/" + vals.length + " to match", val, store.get("my_partition", key));
        }
        log.info("parallel: done");
    }
}
