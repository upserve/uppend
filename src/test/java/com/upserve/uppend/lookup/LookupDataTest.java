package com.upserve.uppend.lookup;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class LookupDataTest {

    private static final String LOOKUP_KEY = "Lookup Key";
    private static final String LOOKUP_PAGES = "Lookup Pages";
    private static final String LOOKUP_METADATA = "Lookup Metadata";

    private final String name = "lookupdata-test";
    private final Path lookupDir = Paths.get("build/test/lookup").resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder
            .getDefaultTestBuilder()
            .withLookupPageSize(32*1024)
            .withMaximumLookupKeyCacheWeight(1024 * 1024)
            ;

    private final FileCache fileCache = defaults.buildFileCache(false, name);
    private final PageCache pageCache = defaults.buildLookupPageCache(fileCache, name);
    private final LookupCache lookupCache = defaults.buildLookupCache(pageCache, name);

    private final PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);

    private AtomicReference<CacheStats> lookupPageCacheStats = new AtomicReference<>(lookupCache.pageStats());
    private AtomicReference<CacheStats> lookupKeyCacheStats = new AtomicReference<>(lookupCache.keyStats());
    private AtomicReference<CacheStats> lookupMetadataCacheStats = new AtomicReference<>(lookupCache.metadataStats());

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeDirectory(lookupDir);
    }

    @After
    public void tearDown() {
        lookupCache.flush();
        pageCache.flush();
        fileCache.flush();
    }

    @Test
    public void testCtor() throws Exception {
        new LookupData(lookupDir, partitionLookupCache);
    }

    @Test
    public void testCtorErrors() throws Exception {
        Files.createDirectories(lookupDir);
        File notDir = File.createTempFile("not-a-dir", ".tmp", lookupDir.toFile());
        Path notDirPath = notDir.toPath();
        Exception expected = null;

        try {
            new LookupData(notDirPath, partitionLookupCache);
        } catch (UncheckedIOException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("unable to make parent dir"));

        expected = null;
        notDirPath = notDirPath.resolve("sub").resolve("sub2");
        try {
            new LookupData(notDirPath, partitionLookupCache);
        } catch (UncheckedIOException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("unable to make parent dir"));

        assertTrue(notDir.delete());
    }

    @Test
    public void testGetAndPut() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        final LookupKey key = new LookupKey("mykey");
        assertEquals(null, data.get(key));
        data.put(key, 80);
        assertEquals(Long.valueOf(80), data.get(key));
    }

    @Test
    public void testPutIfNotExists() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, 1);
        assertEquals(Long.valueOf(1), data.get(key));
        data.putIfNotExists(key, 2);
        assertEquals(Long.valueOf(1), data.get(key));
    }

    @Test
    public void testPutIfNotExistsFunction() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, () -> 1);
        assertEquals(Long.valueOf(1), data.get(key));
        data.putIfNotExists(key, () -> 2);
        assertEquals(Long.valueOf(1), data.get(key));
    }

    @Test
    public void testFlushAndClose() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        final LookupKey key = new LookupKey("mykey");
        data.put(key, 80);
        data.flush();
        data = new LookupData(lookupDir, partitionLookupCache);
        assertEquals(Long.valueOf(80), data.get(key));
    }

    @Test
    public void testCachePutSupplierIfNotExistFlush() throws IOException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

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

        assertEquals(null, data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(1, 2, 2, 0);
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
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCachePutValIfNotExistFlush() throws IOException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

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

        assertEquals(null, data.writeCache.get(key));

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(1, 2, 2, 0);
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
        assertLookupPagesCache(0, 1, 1, 0);
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCachePutFlush() throws IOException, InterruptedException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

        final LookupKey key = new LookupKey("mykey");
        assertEquals(null, data.put(key, 80));

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

        assertEquals(null, data.writeCache.get(key)); // the key has been written and moved from the write cache to the read cache

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(1, 2, 2, 0); // Keys blob store and LongLongStore
        assertLookupMetadataCache(1, 0, 0, 0); // Metadata is loaded during flush

        // put a new value and see which cache entries change
        assertEquals(Long.valueOf(81), data.put(key, 82));

        assertEquals(null, data.writeCache.get(key)); // Write cache is only for new keys.

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(1, 0, 0, 0); // loads the page to write to
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        assertEquals(Long.valueOf(82), data.put(key, 83));

        assertEquals(null, data.writeCache.get(key)); // Write cache is only for new keys

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(1, 1, 1, 0); // load the page, then update
        assertLookupMetadataCache(0, 1, 1, 0);
    }

    @Test
    public void testCacheIncrementFlush() throws IOException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

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

        assertEquals(null, data.writeCache.get(key)); // the key has been written and moved from the write cache to the read cache

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(1, 2, 2, 0); // Keys blob store and LongLongStore
        assertLookupMetadataCache(1, 0, 0, 0); // Metadata is loaded during flush

        // put a new value and see which cache entries change
        assertEquals(36, data.increment(key, 12));

        assertEquals(null, data.writeCache.get(key)); // Write cache is only for new keys.

        assertLookupKeyCache(1, 0, 0, 0);
        assertLookupPagesCache(1, 0, 0, 0); // loads the page to write to
        assertLookupMetadataCache(0, 0, 0, 0);

        lookupCache.flush();

        assertLookupKeyCache(0, 0, 0, 0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);

        assertEquals(48, data.increment(key, 12));

        assertEquals(null, data.writeCache.get(key)); // Write cache is only for new keys

        assertLookupKeyCache(0, 1, 1, 0);
        assertLookupPagesCache(1, 1, 1, 0); // load the page, then update
        assertLookupMetadataCache(0, 1, 1, 0);
    }


    // Test helpers
    private void assertLookupKeyCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = lookupCache.keyStats();
        assertCache(LOOKUP_KEY, current.minus(lookupKeyCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertLookupPagesCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = lookupCache.pageStats();
        assertCache(LOOKUP_PAGES, current.minus(lookupPageCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertLookupMetadataCache(long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        CacheStats current = lookupCache.metadataStats();
        assertCache(LOOKUP_METADATA, current.minus(lookupMetadataCacheStats.getAndSet(current)), hitCount, missCount, loadSuccessCount, loadFailureCount);
    }

    private void assertCache(String name, CacheStats stats, long hitCount, long missCount, long loadSuccessCount, long loadFailureCount) {
        assertEquals(name + " Cache Hit Count", hitCount, stats.hitCount());
        assertEquals(name + " Cache Miss Count", missCount, stats.missCount());
        assertEquals(name + " Cache Load Success Count", loadSuccessCount, stats.loadSuccessCount());
        assertEquals(name + " Cache Load Failure Count", loadFailureCount, stats.loadFailureCount());
    }

    @Test
    public void testWriteCacheUnderLoad() throws IOException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

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
        assertLookupPagesCache(0, 77, 77, 0);
        lookupPageCacheStats.set(lookupCache.getPageCache().stats());
        assertLookupMetadataCache(1, 0, 0, 0);


        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        assertLookupKeyCache(100_000, 0, 0,  0);
        assertLookupPagesCache(0, 0, 0, 0);
        assertLookupMetadataCache(0, 0, 0, 0);
    }



//    @Test
//    public void testScan() throws Exception {
//        LookupData data = new LookupData(lookupDir, partitionLookupCache);
//        data.put(new LookupKey("mykey1"), 1);
//        data.put(new LookupKey("mykey2"), 2);
//        data.flush();
//        Map<String, Long> entries = new TreeMap<>();
//        data.scan(entries::put);
//        assertEquals(2, entries.size());
//        assertArrayEquals(new String[]{"mykey1", "mykey2"}, entries.keySet().toArray(new String[0]));
//        assertArrayEquals(new Long[]{1L, 2L}, entries.values().toArray(new Long[0]));
//    }
//
//    @Test
//    public void testScanNonExistant() throws Exception {
//        LookupData data = new LookupData(lookupDir, partitionLookupCache);
//        data.scan((k, v) -> {
//            throw new IllegalStateException("should not have called this");
//        });
//    }
}
