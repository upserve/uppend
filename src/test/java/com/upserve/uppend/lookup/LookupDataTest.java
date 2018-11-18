package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.primitives.Ints;
import com.upserve.uppend.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.*;

import static org.junit.Assert.*;

public class LookupDataTest {

    private static final int RELOAD_INTERVAL = -1;
    private static final int FLUSH_THRESHOLD = -1;

    private final String name = "lookupdata-test";
    private final Path lookupDir = Paths.get("build/test/lookup").resolve(name);
    private AppendOnlyStoreBuilder defaults = TestHelper
            .getDefaultAppendStoreTestBuilder();

    private VirtualPageFile metadataPageFile;
    private VirtualMutableBlobStore mutableBlobStore;

    private VirtualPageFile keyDataPageFile;
    private VirtualLongBlobStore keyBlobStore;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public static final int NUMBER_OF_STORES = 12;

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeDirectory(lookupDir);
        Files.createDirectories(lookupDir);
        setup(false);
    }


    public void setup(boolean readOnly) {
        metadataPageFile = new VirtualPageFile(lookupDir.resolve("metadata"), NUMBER_OF_STORES, 1024, 16384, readOnly);
        mutableBlobStore = new VirtualMutableBlobStore(1, metadataPageFile);

        keyDataPageFile = new VirtualPageFile(lookupDir.resolve("keydata"), NUMBER_OF_STORES, defaults.getLookupPageSize(), defaults.getTargetBufferSize(), readOnly);
        keyBlobStore = new VirtualLongBlobStore(1, keyDataPageFile);
    }

    @After
    public void tearDown() throws IOException {
        keyDataPageFile.close();
        metadataPageFile.close();
    }

    @Test
    public void testOpenEmptyReadOnly() throws IOException {
        tearDown(); // Close the page files
        setup(true);
        LookupData data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));

        thrown.expect(RuntimeException.class);
        data.put(key, 1L);
    }

    @Test
    public void testOpenGetAndPut() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));
        data.put(key, 80);
        assertEquals(Long.valueOf(80), data.getValue(key));
    }

    @Test
    public void testPutIfNotExists() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.putIfNotExists(key, 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testPutIfNotExistsFunction() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.putIfNotExists(key, () -> 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, () -> 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testPut() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.put(key, 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.put(key, 2);
        assertEquals(Long.valueOf(2), data.getValue(key));
    }

    @Test
    public void testIncrement() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.increment(key, 10);
        assertEquals(Long.valueOf(10), data.getValue(key));
        data.increment(key, 2);
        assertEquals(Long.valueOf(12), data.getValue(key));
    }



    @Test
    public void testFlushAndClose() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        data.put(key, 80);
        data.flush();

        Long result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);

        tearDown();
        setup(true);

        data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
        result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);
    }

    @Test
    public void testWriteCacheUnderLoad() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });


        assertEquals(100_000, data.writeCache.size());

        data.flush();

        assertEquals(0, data.writeCache.size());

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        LongStream.range(0, 100_010)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        assertEquals(10, data.writeCache.size());


    }

    @Test
    public void testScan() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        LookupKey firstKey = new LookupKey("mykey1");
        LookupKey secondKey = new LookupKey("mykey2");

        data.put(firstKey, 1);
        data.put(secondKey, 2);

        assertEquals(0, data.flushCache.size());
        assertEquals(2, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.flushWriteCache(data.getMetadata());

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.generateMetaData(data.getMetadata());

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.flushCache.clear();

        assertEquals(0, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        tearDown();
        setup(true);

        data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});
    }

    private void scanTestHelper(LookupData data, LookupKey[] expectedKeys, Long[] expectedValues) {
        ConcurrentMap<LookupKey, Long> entries = new ConcurrentHashMap<>();
        data.scan(entries::put);
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
        assertArrayEquals(expectedValues, entries.values().stream().sorted().toArray(Long[]::new));

        entries = data.scan().collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
        assertArrayEquals(expectedValues, entries.values().stream().sorted().toArray(Long[]::new));

        entries = data.keys().collect(Collectors.toConcurrentMap(Function.identity(), v -> 1L));
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
    }

    @Test
    public void testScanNonExistant() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        data.scan((k, v) -> {
            throw new IllegalStateException("should not have called this");
        });

        data.scan().forEach(entry -> {
            throw new IllegalStateException("should not have called this");
        });

        data.keys().forEach(key -> {
            throw new IllegalStateException("should not have called this");
        });
    }

    @Test
    public void testLoadReadOnlyMetadata() {
        LookupData data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Check checksum

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Checksum did not match for the requested blob");

        data.getValue(new LookupKey("foobar"));
    }

    @Test
    public void testLoadReadRepairMetadata() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);

        Random random = new Random();
        LongStream.range(0, 100_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = new byte[(int) (val % 64)];
                    random.nextBytes(bytes);
                    data.put(new LookupKey(bytes), val);
                });

        data.flush();

        LookupMetadata expected = data.getMetadata();


        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Invalid Check checksum

        // Do read repair!
        LookupMetadata result = data.loadMetadata();

        // It is a new object!
        assertNotEquals(expected, result);

        assertEquals(expected.getMinKey(), result.getMinKey());
        assertEquals(expected.getMaxKey(), result.getMaxKey());

        assertArrayEquals(expected.getKeyStorageOrder(), result.getKeyStorageOrder());
    }

    @Test
    public void testFlushWithAppendLoad() throws ExecutionException, InterruptedException {


        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, 100);

        int n = 500;

        Thread flusher = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                data.flush();
            }
        });

        Random random = new Random();
        Thread writer = new Thread(() -> {
            for (int j = 0; j < n; j++) {
                random.longs(128, 0 , 1000)
                        .parallel()
                        .forEach(val -> {
                            byte[] bytes = new byte[(int) (val % 64)];
                            random.nextBytes(bytes);
                            data.put(new LookupKey(bytes), val);
                        });
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
        });

        writer.start();
        flusher.start();

        writer.join();
    }
}
