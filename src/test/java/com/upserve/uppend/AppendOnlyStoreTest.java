package com.upserve.uppend;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.upserve.uppend.lookup.LongLookup;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class AppendOnlyStoreTest {
    private AppendOnlyStore newStore() {
        return new AppendOnlyStoreBuilder().withDir(Paths.get("build/test/file-append-only-store")).withLongLookupHashSize(8).build(false);
    }

    private AppendOnlyStore store;

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
        store.clear();
        assertEquals(0, store.read("partition", key).count());
        store.append("partition", key, bytes);
        assertEquals(1, store.read("partition", key).count());
    }

    @Test
    public void testPurge() throws Exception {
        String key = "foobar";

        byte[] bytes = genBytes(12);
        store.append("partition", key, bytes);
        store.trim();
        store.append("partition", key, bytes);

        assertEquals(2, store.read("partition", key).count());
    }


    @Test
    public void testAppendWhileFlushing() throws Exception {
        // This test is currently slow but it works.
        // If you purge instead of flush it locks
        // Concurrent implementation of LongLookup will fix this issue
        ConcurrentHashMap<String, ArrayList<Long>> testData = new ConcurrentHashMap<>();

        Thread flusherThread = new Thread(() -> {
            while (true) {
                store.flush();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        flusherThread.start();

        new Random(314159)
                .longs(50_000, 0, 1000)
                .parallel()
                .forEach(val -> {
                    String key = String.valueOf(val);

                    testData.compute(key, (k, list) -> {
                        if (list == null) {
                            list = new ArrayList<>();
                        }
                        list.add(val);

                        store.append("_" + key.substring(0, 1), key, Longs.toByteArray(val));

                        assertArrayEquals(
                                list.stream().mapToLong(v -> v).toArray(),
                                store.read("_" + key.substring(0, 1), key).mapToLong(Longs::fromByteArray).toArray()
                        );

                        return list;
                    });

                });

        Thread.sleep(100);

        flusherThread.interrupt();
        flusherThread.join();
    }

    @Test
    public void testClearThenClose() throws Exception {
        store.clear();
        store.close();
    }

    @Test
    public void testWriteThenClearThenClose() throws Exception {
        store.append("partition", "foo", "bar".getBytes());
        store.clear();
        store.close();
    }

    @Test
    public void fillTheCache() {
        int keys = LongLookup.DEFAULT_WRITE_CACHE_SIZE * 2;

        Random random = new Random(9876);
        Set<String> uuidSet = new HashSet<>();
        while (uuidSet.size() < keys) {
            uuidSet.add(new UUID(random.nextLong(), random.nextLong()).toString());
        }
        String[] uuids = uuidSet.toArray(new String[0]);

        Arrays.stream(uuids)
                .parallel()
                .forEach(uuid -> store.append("_" + uuid.substring(0, 2), uuid, uuid.getBytes()));

        cleanUp();
        store = newStore();
        String[] uuids2 = Arrays.copyOf(uuids, uuids.length);
        Collections.shuffle(Arrays.asList(uuids2));
        Arrays.stream(uuids2)
                .parallel()
                .forEach(uuid -> {
                    byte[][] results = store.read("_" + uuid.substring(0, 2), uuid).toArray(byte[][]::new);
                    assertEquals("uuid failed to return 1 result: " + uuid, 1, results.length);
                    byte[] bytes = results[0];
                    assertArrayEquals("uuid result failed to check out: " + uuid, bytes, uuid.getBytes());
                });
    }

    @Test
    public void testReadStream() throws Exception {
        store.append("partition", "stream", "bar".getBytes());
        store.append("partition", "stream", "baz".getBytes());
        assertArrayEquals(new String[]{"bar", "baz"}, store.read("partition", "stream").map(String::new).sorted().toArray(String[]::new));
    }

    @Test
    public void testReadLast() throws Exception {
        store.append("partition", "stream", "foo".getBytes());
        store.append("partition", "stream", "bar".getBytes());
        store.append("partition", "stream", "baz".getBytes());
        byte[] valueBytes = store.readLast("partition", "stream");
        assertNotNull(valueBytes);
        String value = new String(valueBytes);
        assertEquals("baz", value);
    }

    @Test
    public void testMultiPartition() throws Exception {
        store.append("partition", "key", "bar".getBytes());
        store.append("partition_bar", "key", "baz".getBytes());
        store.append("partition2", "key", "bap".getBytes());
        assertArrayEquals(new String[]{"bar"}, store.read("partition", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[]{"baz"}, store.read("partition_bar", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[]{"bap"}, store.read("partition2", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[]{}, store.read("partition3", "key").map(String::new).toArray(String[]::new));
    }

    @Test
    public void testSpecialCharacterPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("partition!", "foo", "bar".getBytes());
    }

    @Test
    public void testLeadingSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("/partition", "foo", "bar".getBytes());
    }

    @Test
    public void testTrailingSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("partition/", "foo", "bar".getBytes());
    }

    @Test
    public void testDotPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("pre.fix", "foo", "bar".getBytes());
    }

    @Test
    public void testEmptyPartPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("foo//bar", "foo", "bar".getBytes());
    }

    @Test
    public void testEmptyPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("", "foo", "bar".getBytes());
    }

    @Test
    public void testJustSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("/", "foo", "bar".getBytes());
    }

    @Test
    public void testJustDashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("-", "foo", "bar".getBytes());
    }

    @Test
    public void testJustSlashesPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("//", "foo", "bar".getBytes());
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
    public void testPartitions() throws Exception {
        store.append("partition_one", "one", "bar".getBytes());
        store.append("partition_two", "two", "baz".getBytes());
        store.append("partition$three", "three", "bop".getBytes());
        store.append("partition-four", "four", "bap".getBytes());
        store.append("_2016-01-02", "five", "bap".getBytes());
        assertArrayEquals(new String[]{"_2016-01-02", "partition$three", "partition-four", "partition_one", "partition_two"}, store.partitions().sorted().toArray(String[]::new));
    }

    @Test
    public void testScan() throws Exception {
        store.append("partition_one", "one", "bar".getBytes());
        store.append("partition_one", "two", "baz".getBytes());
        store.append("partition_one", "three", "bop".getBytes());
        store.append("partition_one", "one", "bap".getBytes());
        store.append("partition_two", "five", "bap".getBytes());

        Map<String, List<String>> result = store
                .scan("partition_one")
                .collect(Collectors
                        .toMap(
                                Map.Entry::getKey,
                                entry -> entry
                                        .getValue()
                                        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                                        .collect(Collectors.toList())
                        )
                );

        Map<String, List<String>> expected = ImmutableMap.of(
                "one", Arrays.asList("bar", "bap"),
                "two", Collections.singletonList("baz"),
                "three", Collections.singletonList("bop")
        );

        assertEquals(expected, result);
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

    private void tester(int number, int size) {
        String key = "foobar";
        String partition = "partition";

        byte[] bytes;
        ArrayList<byte[]> inputBytes = new ArrayList<>();
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
        return Integer.compare(o1.length, o2.length);
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        new Random().nextBytes(bytes);
        return bytes;
    }
}
