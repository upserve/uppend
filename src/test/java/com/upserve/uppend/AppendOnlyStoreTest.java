package com.upserve.uppend;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AppendOnlyStoreTest {
    protected abstract AppendOnlyStore newStore();

    protected AppendOnlyStore store;

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

    public void testReservations(Path path, Function<Path, AppendOnlyStore> supplier) throws Exception {
        AppendOnlyStore store1 = supplier.apply(path);
        AppendOnlyStore store2 = null;

        try {
            store2 = supplier.apply(path);
            fail("Opening a second store instance for the same DB should fail");
        } catch (IllegalStateException e) {

        } finally {
            if (store1 != null) store1.close();
            if (store2 != null) store2.close();
        }
    }

    @Test
    public void testWriteCloseReadRepeat() throws Exception {
        store.append("partition", "foo", "bar".getBytes());
        store.close();
        store = newStore();
        store.append("partition", "foo", "baz".getBytes());
        store.append("partition", "qux", "xyzzy".getBytes());
        store.close();
        store = newStore();
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        store.read("partition", "foo").map(String::new).forEach(results::add);
        assertArrayEquals(new String[]{"bar", "baz"}, results.stream().sorted().toArray(String[]::new));
        results.clear();
        store.read("partition", "qux").map(String::new).forEach(results::add);
        assertArrayEquals(new String[]{"xyzzy"}, results.stream().sorted().toArray(String[]::new));
    }

    @Test
    public void testClear() throws Exception {
        String key = "foobar";
        byte[] bytes = genBytes(12);
        store.append("partition", key, bytes);
        flush();
        store.clear();
        assertEquals(0, store.read("partition", key).count());
    }

    @Test
    public void fillTheCache() {
        // MAX_LOOKUPS_CACHE_SIZE * 2 keys ensures cache will be filled
        int keys = 4096 * 2;

        List<String> uuids = IntStream
                .range(0, keys)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());

        Random random = new Random(9876);
        random
                .ints(keys * 10, 0, keys)
                .parallel()
                .forEach( i -> {
                    String uuid = uuids.get(i);
                    store.append(uuid.substring(0, 2), uuid, uuid.getBytes());
                });

        uuids
                .stream()
                .parallel()
                .forEach(uuid ->
                        store
                                .read(uuid.substring(0, 2), uuid)
                                .findFirst()
                                .ifPresent(bytes -> assertArrayEquals(bytes, uuid.getBytes()))
                );
    }

    @Test
    public void testReadStream() throws Exception {
        store.append("partition", "stream", "bar".getBytes());
        store.append("partition", "stream", "baz".getBytes());
        flush();
        assertArrayEquals(new String[] { "bar", "baz" }, store.read("partition", "stream").map(String::new).sorted().toArray(String[]::new));
    }

    @Test
    public void testMultiPartition() throws Exception {
        store.append("partition", "key", "bar".getBytes());
        store.append("partition/bar", "key", "baz".getBytes());
        store.append("partition2", "key", "bap".getBytes());
        flush();
        assertArrayEquals(new String[] { "bar" }, store.read("partition", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[] { "baz" }, store.read("partition/bar", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[] { "bap" }, store.read("partition2", "key").map(String::new).toArray(String[]::new));
    }
    
    @Test
    public void testRead_BadPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.read("bad*partition", "stream");
    }

    @Test
    public void testAppend_BadPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("bad*partition", "stream", new byte[]{1});
    }

    @Test
    public void testKeys() throws Exception {
        store.append("partition", "one", "bar".getBytes());
        store.append("partition", "two", "baz".getBytes());
        store.append("partition2", "two", "baz".getBytes());
        store.append("partition2", "three", "baz".getBytes());
        store.close();
        store = newStore();
        assertArrayEquals(new String[]{"one", "two"}, store.keys("partition").sorted().toArray(String[]::new));
    }

    @Test
    public void testKeys_BadPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.keys("bad*partition");
    }

    @Test
    public void testPartitions() throws Exception {
        store.append("partition_one", "one", "bar".getBytes());
        store.append("partition_two", "two", "baz".getBytes());
        store.append("partition/three", "three", "bop".getBytes());
        store.append("partition-four", "four", "bap".getBytes());
        store.append("2016-01-02", "five", "bap".getBytes());
        assertArrayEquals(new String[]{"2016-01-02", "partition-four", "partition/three", "partition_one", "partition_two"}, store.partitions().sorted().toArray(String[]::new));
    }

    @Test
    public void testReadWriteSingle() {
        tester(1, 17);
    }

    @Test
    public void testReadWriteLarge() {
        tester(1, 1028);
    }

    @Test
    public void testReadWriteMulti() {
        tester(20, 17);
    }

    @Test
    public void testReadWriteMore() {
        tester(40, 17);
    }

    @Test
    public void testReadWriteZero() {
        tester(0, 17);
    }

    @Test
    public void testReadWriteEmpty() {
        tester(1, 0);
    }

    public void tester(int number, int size) {
        String key = "foobar";
        String partition = "partition";

        byte[] bytes;
        ArrayList<byte[]> inputBytes = new ArrayList();
        for (int i = 0; i < number; i++) {
            bytes = genBytes(size);
            inputBytes.add(bytes);
            store.append(partition, key, bytes);
        }

        try {
            store.close();
        } catch (Exception e) {
            throw new AssertionError("Should not raise: {}", e);
        }

        store = newStore();

        List<byte[]> outputBytes = Collections.synchronizedList(new ArrayList<>());

        store.read(partition, key).forEach(outputBytes::add);

        assertEquals(inputBytes.size(), outputBytes.size());

        inputBytes.sort(AppendOnlyStoreTest::compareByteArrays);
        outputBytes.sort(AppendOnlyStoreTest::compareByteArrays);

        for (int i = 0; i < number; i++) {
            assertArrayEquals("input and output byte arrays differ at index " + i, inputBytes.get(i), outputBytes.get(i));
        }
    }

    private static int compareByteArrays(byte[] o1, byte[] o2) {
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            }
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        for (int i = 0; i < o1.length && i < o2.length; i++) {
            int v1 = 0xff & o1[i];
            int v2 = 0xff & o2[i];
            if (v1 != v2) {
                return v1 < v2 ? -1 : 1;
            }
        }
        if (o1.length == o2.length) {
            return 0;
        }
        if (o1.length < o2.length) {
            return -1;
        }
        return 1;
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[12];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private void flush() throws Exception {
        try {
            store.close();
        } catch (Exception e){
            throw new AssertionError("close should not raise: {}", e);
        }
        store = newStore();
    }
}
