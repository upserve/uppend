package com.upserve.uppend.lookup;

import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

public class LookupDataTest {

    private final String name = "lookupdata-test";
    private final Path lookupDir = Paths.get("build/test/lookup").resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder();

    private final FileCache fileCache = defaults.buildFileCache(false, name);
    private final PageCache pageCache = defaults.buildLookupPageCache(fileCache, name);
    private final LookupCache lookupCache = defaults.buildLookupCache(pageCache, name);

    private final PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);

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
    public void testCachePutFlush() throws IOException, InterruptedException {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);

        final LookupKey key = new LookupKey("mykey");
        assertEquals(null, data.put(key, 80));

        assertEquals(Long.valueOf(80), data.writeCache.get(key));

        assertEquals(1, lookupCache.keyStats().missCount());
        assertEquals(0, lookupCache.keyStats().hitCount());

        assertEquals(0, lookupCache.pageStats().requestCount());

        assertEquals(1, lookupCache.metadataStats().missCount());
        assertEquals(0, lookupCache.metadataStats().hitCount());

        // Updating the value while still in the write cache changes nothing else
        assertEquals(Long.valueOf(80), data.put(key, 81));

        assertEquals(1, lookupCache.keyStats().missCount());
        assertEquals(0, lookupCache.keyStats().hitCount());

        assertEquals(0, lookupCache.pageStats().requestCount());

        assertEquals(1, lookupCache.metadataStats().missCount());
        assertEquals(0, lookupCache.metadataStats().hitCount());

        // Flush the key to disk and more the key/value to the read cache
        data.flush();

        assertEquals(1, lookupCache.keyStats().requestCount()); // unchanged

        assertEquals(2, lookupCache.pageStats().missCount()); // Keys blob store and LongLongStore
        assertEquals(1, lookupCache.pageStats().hitCount());

        assertEquals(1, lookupCache.metadataStats().missCount());
        assertEquals(1, lookupCache.metadataStats().hitCount()); // Metadata is loaded during flush

        assertEquals(null, data.writeCache.get(key)); // the key has been written and moved from the write cache to the read cache

        // put a new value and see which cache entries change
        assertEquals(Long.valueOf(81), data.put(key, 82));

        assertEquals(null, data.writeCache.get(key)); // Write cache is only for new keys.

        assertEquals(1, lookupCache.keyStats().hitCount()); // Should be a hit!
        assertEquals(1, lookupCache.keyStats().missCount());

        assertEquals(2, lookupCache.pageStats().hitCount()); // writing the new value is a hit
        assertEquals(2, lookupCache.pageStats().missCount()); // miss count unchanged

        assertEquals(1, lookupCache.metadataStats().missCount()); // metadata is untouched.
        assertEquals(1, lookupCache.metadataStats().hitCount());
    }


    @Test
    public void testScan() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        data.put(new LookupKey("mykey1"), 1);
        data.put(new LookupKey("mykey2"), 2);
        data.flush();
        Map<String, Long> entries = new TreeMap<>();
        data.scan(entries::put);
        assertEquals(2, entries.size());
        assertArrayEquals(new String[] {"mykey1", "mykey2"}, entries.keySet().toArray(new String[0]));
        assertArrayEquals(new Long[] {1L, 2L}, entries.values().toArray(new Long[0]));
    }

    @Test
    public void testScanNonExistant() throws Exception {
        LookupData data = new LookupData(lookupDir, partitionLookupCache);
        data.scan((k, v) -> {
            throw new IllegalStateException("should not have called this");
        });
    }
}
