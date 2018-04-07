package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

public class LookupDataTest {
    private Path lookupDir = Paths.get("build/test/tmp/lookup-data");

    private final FileCache fileCache = new FileCache(64, 256, false);
    private final PagedFileMapper pageCache = new PagedFileMapper(256*1024, 16, 64, fileCache);
    private final LookupCache lookupCache = new LookupCache(pageCache);
    private final PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);
    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(lookupDir);
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
        assertTrue(expected.getMessage().contains("can't open file"));

        expected = null;
        notDirPath = notDirPath.resolve("sub").resolve("sub2");
        try {
            new LookupData(notDirPath, partitionLookupCache);
        } catch (UncheckedIOException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("unable to make parent dir"));

        Files.write(lookupDir.resolve("data"), "short bad data".getBytes());
        new LookupData(lookupDir, partitionLookupCache);

        Files.write(lookupDir.resolve("data"), "bad data that is long enough to cross record".getBytes());
        expected = null;
        try {
            new LookupData(lookupDir, partitionLookupCache);
        } catch (BufferUnderflowException e) {
            expected = e;
        }
        assertNotNull(expected);

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
