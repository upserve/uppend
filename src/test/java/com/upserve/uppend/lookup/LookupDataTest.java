package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.primitives.Ints;
import com.upserve.uppend.AppendOnlyStoreBuilder;
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

    private static final String LOOKUP_KEY = "Lookup Key";
    private static final String LOOKUP_PAGES = "Lookup Pages";
    private static final String LOOKUP_METADATA = "Lookup Metadata";

    private final String name = "lookupdata-test";
    private final Path lookupDir = Paths.get("build/test/lookup").resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder
            .getDefaultTestBuilder()
            .withMaximumLookupKeyCacheWeight(1024 * 1024);

    private final PageCache pageCache = defaults.buildLookupPageCache(name);
    private final LookupCache lookupCache = defaults.buildLookupCache(name);

    private final PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);

    private AtomicReference<CacheStats> lookupPageCacheStats = new AtomicReference<>(pageCache.stats());
    private AtomicReference<CacheStats> lookupKeyCacheStats = new AtomicReference<>(lookupCache.keyStats());
    private AtomicReference<CacheStats> lookupMetadataCacheStats = new AtomicReference<>(lookupCache.metadataStats());

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
        metadataPageFile = new VirtualPageFile(lookupDir.resolve("metadata"), NUMBER_OF_STORES, 1024, readOnly);
        mutableBlobStore = new VirtualMutableBlobStore(1, metadataPageFile);

        keyDataPageFile = new VirtualPageFile(lookupDir.resolve("keydata"), NUMBER_OF_STORES, readOnly, pageCache);
        keyBlobStore = new VirtualLongBlobStore(1, keyDataPageFile);
    }

    @After
    public void tearDown() throws IOException {
        lookupCache.flush();
        pageCache.flush();
        keyDataPageFile.close();
        metadataPageFile.close();
    }

    @Test
    public void testOpenEmptyReadOnly() throws IOException {
        tearDown(); // Close the page files
        setup(true);
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, true);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));

        thrown.expect(RuntimeException.class);
        data.put(key, 1L);
    }

    @Test
    public void testOpenGetAndPut() {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));
        data.put(key, 80);
        assertEquals(Long.valueOf(80), data.getValue(key));
    }

    @Test
    public void testPutIfNotExists() {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testPutIfNotExistsFunction() {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, () -> 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, () -> 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testFlushAndClose() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
        final LookupKey key = new LookupKey("mykey");
        data.put(key, 80);
        data.flush();

        lookupCache.flush();
        pageCache.flush();

        Long result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);

        tearDown();
        setup(true);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, true);
        result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);
    }

    @Test
    public void testCachePutSupplierIfNotExistFlush() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        final LookupKey key = new LookupKey("mykey");
        assertEquals(16, data.putIfNotExists(key, () -> 16L));

        assertEquals(Long.valueOf(16), data.writeCache.get(key));

        assertLookupKeyCache(0, 1, 0, 1);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        // ignores new value - nothing changes
        assertEquals(16, data.putIfNotExists(key, () -> 17L));

        assertEquals(Long.valueOf(16), data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        // Flush the write Cache and put the key in the read cache
        data.flush();

        assertNull(data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 1, 0, 0);
        assertLookupMetadataCache(1, 0, 0, 0);

        // call put if not exist again with the data on disk and in the read caches
        assertEquals(16, data.putIfNotExists(key, () -> 17L));

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        // call put if not exist again with the data on disk but not in the read cache
        assertEquals(16, data.putIfNotExists(key, () -> 17L));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        tearDown();
        setup(false);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        assertEquals(16, data.putIfNotExists(key, () -> 18L));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCachePutValIfNotExistFlush() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        final LookupKey key = new LookupKey("mykey");
        assertEquals(80, data.putIfNotExists(key, 80));

        assertEquals(Long.valueOf(80), data.writeCache.get(key));

        assertLookupKeyCache(0, 1, 0, 1);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        // ignores new value - nothing changes
        assertEquals(80, data.putIfNotExists(key, 86));

        assertEquals(Long.valueOf(80), data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        // Flush the write Cache and put the key in the read cache
        data.flush();

        assertNull(data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 1, 0, 0);
        assertLookupMetadataCache(1, 0, 0, 0);

        // call put if not exist again with the data on disk and in the read caches
        assertEquals(80, data.putIfNotExists(key, 86));

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        // call put if not exist again with the data on disk but not in the read cache
        assertEquals(80, data.putIfNotExists(key, 86));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        tearDown();
        setup(false);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        assertEquals(80, data.putIfNotExists(key, 87));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCachePutFlush() throws IOException, InterruptedException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        final LookupKey key = new LookupKey("mykey");
        assertNull(data.put(key, 80));

        assertEquals(Long.valueOf(80), data.writeCache.get(key));

        assertLookupKeyCache(0, 1, 0, 1);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        // Updating the value while still in the write cache changes nothing else
        assertEquals(Long.valueOf(80), data.put(key, 81));

        assertEquals(Long.valueOf(81), data.writeCache.get(key)); // write cache is updated...

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);


        // Flush the key to disk and more the key/value to the read cache
        data.flush();

        assertNull(data.writeCache.get(key)); // the key has been written and moved from the write cache to the read cache

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 1, 0, 0);
        assertLookupMetadataCache(1, 0, 0, 0);

        // put a new value and see which cache entries change
        assertEquals(Long.valueOf(81), data.put(key, 82));

        assertNull(data.writeCache.get(key)); // Write cache is only for new keys.

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        assertEquals(Long.valueOf(82), data.put(key, 83));

        assertNull(data.writeCache.get(key)); // Write cache is only for new keys

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        tearDown();
        setup(false);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        assertEquals(Long.valueOf(83), data.put(key, 84));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCacheIncrementFlush() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        final LookupKey key = new LookupKey("mykey");
        assertEquals(12, data.increment(key, 12));

        assertEquals(Long.valueOf(12), data.writeCache.get(key));

        assertLookupKeyCache(0, 1, 0, 1);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 1, 1, 0);

        // Updating the value while still in the write cache changes nothing else
        assertEquals(24, data.increment(key, 12));

        assertEquals(Long.valueOf(24), data.writeCache.get(key)); // write cache is updated...

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);


        // Flush the key to disk and more the key/value to the read cache
        data.flush();

        assertNull(data.writeCache.get(key)); // the key has been written and moved from the write cache to the read cache

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 1, 0, 0); // Keys blob store and LongLongStore
        assertLookupMetadataCache(1, 0, 0, 0); // Metadata is loaded during flush

        // put a new value and see which cache entries change
        assertEquals(36, data.increment(key, 12));

        assertNull(data.writeCache.get(key)); // Write cache is only for new keys.

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0); // loads the page to write to
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        assertEquals(48, data.increment(key, 12));

        assertNull(data.writeCache.get(key)); // Write cache is only for new keys

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 0, 0, 0); // load the page, then update
        assertLookupMetadataCache(0, 1, 1, 0);

        tearDown();
        setup(false);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        assertEquals(60, data.increment(key, 12));

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    // Test helpers
    private void assertLookupKeyCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = lookupCache.keyStats();
        assertCache(LOOKUP_KEY, current.minus(lookupKeyCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertLookupPagesCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = pageCache.stats();
        assertCache(LOOKUP_PAGES, current.minus(lookupPageCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertLookupMetadataCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = lookupCache.metadataStats();
        assertCache(LOOKUP_METADATA, current.minus(lookupMetadataCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertCache(String name, CacheStats stats, long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        if (hitCount > 0) assertEquals(name + " Cache Hit Count", hitCount, stats.hitCount());
        if (missCount > 0) assertEquals(name + " Cache Miss Count", missCount, stats.missCount());
        if (loadSuccessCount > 0) assertEquals(name + " Cache Load Success Count", loadSuccessCount, stats.loadSuccessCount());
        if (loadFailureCount > 0) assertEquals(name + " Cache Load Failure Count", loadFailureCount, stats.loadFailureCount());
    }

    @Ignore
    @Test
    public void testWriteCacheUnderLoad() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });


        assertEquals(100_000, data.writeCache.size());

        assertLookupKeyCache(0, 100_000, 0, 100_000);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(99_999, 1, 1, 0);

        data.flush();

        assertEquals(0, data.writeCache.size());

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 104, 0, 0);
        assertLookupMetadataCache(1, 0, 0, 0);

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        assertLookupKeyCache(100_000, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();
        pageCache.flush();

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        assertLookupKeyCache(0, 100_000, 100_000, 0);
        assertLookupPagesCache(233307, 104, 104, 0);
        assertLookupMetadataCache(99999, 1, 1, 0);
    }

    @Test
    public void testScan() throws IOException {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
        LookupKey firstKey = new LookupKey("mykey1");
        LookupKey secondKey = new LookupKey("mykey2");

        data.put(firstKey, 1);
        data.put(secondKey, 2);

        // Assert that scan works properly during each each phase of the flush operation
        LookupMetadata currentMetadata = partitionLookupCache.getMetadata(data);

        assertEquals(0, data.flushCache.size());
        assertEquals(2, data.writeCache.size());

        data.flushWriteCache(currentMetadata);
        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        data.generateMetaData(currentMetadata);
        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        data.flushCacheToReadCache();
        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        assertEquals(0, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        tearDown();
        setup(true);

        data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
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
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);
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
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, true);

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Check checksum

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Checksum did not match for the requested blob");

        data.getValue(new LookupKey("foobar"));
    }

    @Test
    public void testLoadReadRepairMetadata() {
        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, partitionLookupCache, false);

        Random random = new Random();
        LongStream.range(0, 100_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = new byte[(int) (val % 64)];
                    random.nextBytes(bytes);
                    data.put(new LookupKey(bytes), val);
                });

        data.flush();

        LookupMetadata expected = lookupCache.getMetadata(data);

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Check checksum

        lookupCache.flush();

        // Do read repair!
        assertNull(data.getValue(new LookupKey("foobarChew - MeToo")));

        LookupMetadata result = lookupCache.getMetadata(data);

        // It is a new object!
        assertNotEquals(expected, result);

        assertEquals(expected.getMinKey(), result.getMinKey());
        assertEquals(expected.getMaxKey(), result.getMaxKey());

        assertArrayEquals(expected.getKeyStorageOrder(), result.getKeyStorageOrder());
    }

    @Test
    public void testFlushWithAppendLoad() throws ExecutionException, InterruptedException {

        // Force the metadata to be reloaded every time it is needed
        LookupCache noCache = defaults.withMaximumMetaDataCacheWeight(0).withMaximumLookupKeyCacheWeight(0).buildLookupCache(name);

        LookupData data = new LookupData(keyBlobStore, mutableBlobStore, PartitionLookupCache.create("partition", noCache), 100, false);

        int n = 500;

        Thread flusher = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                data.flush();
            }
        });


        Random random = new Random();
        Thread writer = new Thread(() ->
                LongStream.range(0, 100_000)
                        .forEach(val -> {
                            byte[] bytes = new byte[(int) (val % 64)];
                            random.nextBytes(bytes);
                            data.put(new LookupKey(bytes), val);
                        })
        );

        writer.start();
        flusher.start();

        writer.join();
    }


}
