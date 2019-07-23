package com.upserve.uppend;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.upserve.uppend.TestHelper.genBytes;
import static org.junit.Assert.*;

public class AppendOnlyStoreTest {
    private final Path path = Paths.get("build/test/file-append-only-store");

    private AppendOnlyStore newStore() {
        return newStore(false);
    }

    private AppendOnlyStore newStore(boolean readOnly) {
        return TestHelper.getDefaultAppendStoreTestBuilder().withDir(path.resolve("store-path")).build(readOnly);
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
    public void testEmptyReadOnlyStore() throws Exception {
        cleanUp();

        store = newStore(true);

        assertEquals(0, store.keyCount());
        assertEquals(0, store.read("foo", "bar").count());
        assertEquals(0, store.scan().count());

        try (AppendOnlyStore readWriteStore = newStore(false)) {

            readWriteStore.append("foo", "bar", "bytes".getBytes());
            readWriteStore.flush();

            assertEquals(1, store.keyCount());
            assertEquals(1, store.read("foo", "bar").count());
            assertEquals(1, store.scan().count());
        }
    }

    @Test
    public void testClear() {
        String key = "foobar";

        byte[] bytes = genBytes(12);
        store.append("partition", key, bytes);
        store.clear();
        assertEquals(0, store.read("partition", key).count());

        assertEquals(0, store.keys().count());

        store.append("partition", key, bytes);
        assertEquals(1, store.read("partition", key).count());
    }

    @Test
    public void testTrim() {
        String key = "foobar";

        byte[] bytes = genBytes(12);
        store.append("partition", key, bytes);
        store.trim();
        store.append("partition", key, bytes);

        assertEquals(2, store.read("partition", key).count());
    }

    @Test
    public void testAppendWhileFlushing() throws Exception {
        ConcurrentHashMap<String, ArrayList<Long>> testData = new ConcurrentHashMap<>();

        ExecutorService executor = new ForkJoinPool();
        Future future = executor.submit(() -> {
            new Random(314159)
                    .longs(500_000, 0, 5_000)
                    .parallel()
                    .forEach(val -> {
                        String key = String.valueOf(val);

                        testData.compute(key, (k, list) -> {
                            if (list == null) {
                                list = new ArrayList<>();
                            }
                            list.add(val + 5);

                            store.append("_" + k.substring(0, 1), k, Longs.toByteArray(val + 5));

                            long[] expected = list.stream().mapToLong(v -> v).toArray();
                            long[] result = store.read("_" + k.substring(0, 1), k).mapToLong(Longs::fromByteArray).toArray();

                            if (expected.length != result.length) {
                                fail("Array lenth does not match");
                            }

                            assertArrayEquals(
                                    expected,
                                    result
                            );

                            return list;
                        });

                    });
        });

        future.get(40_000, TimeUnit.MILLISECONDS);

        executor.shutdown();
    }

    @Test
    public void testKeyCount() {
        assertEquals(0, store.keyCount());
        store.append("foo", "bar", "bytes".getBytes());
        store.append("foo", "bar", "bytes".getBytes());
        store.append("foo", "barone", "bytes".getBytes());
        store.append("footwo", "barone", "bytes".getBytes());
        assertEquals(0, store.keyCount());
        store.flush();
        assertEquals(3, store.keyCount());
        store.clear();
        assertEquals(0, store.keyCount());
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
    public void fillTheCache() throws Exception {
        int keys = 1024 * 256;

        Random random = new Random(9876);
        Set<String> uuidSet = new HashSet<>();
        while (uuidSet.size() < keys) {
            uuidSet.add(new UUID(random.nextLong(), random.nextLong()).toString());
        }
        String[] uuids = uuidSet.toArray(new String[0]);

        Arrays.stream(uuids)
                .parallel()
                .forEach(uuid -> store.append("_" + uuid.substring(0, 2), uuid, uuid.getBytes()));

        store.close();
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
        assertArrayEquals(new String[]{"one", "three", "two", "two"}, store.keys().sorted().toArray(String[]::new));
    }

    @Test
    public void testScan() throws Exception {
        store.append("partition_one", "one", "bar".getBytes());
        store.append("partition_one", "two", "baz".getBytes());
        store.append("partition_one", "three", "bop".getBytes());
        store.append("partition_one", "one", "bap".getBytes());
        store.append("partition_two", "five", "bap".getBytes());

        Map<String, List<String>> result = store
                .scan()
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
                "three", Collections.singletonList("bop"),
                "five", Collections.singletonList("bap")
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

        inputBytes.sort(TestHelper::compareByteArrays);
        outputBytes.sort(TestHelper::compareByteArrays);

        for (int i = 0; i < number; i++) {
            assertArrayEquals("input and output byte arrays differ at index " + i, inputBytes.get(i), outputBytes.get(i));
        }
    }
}
