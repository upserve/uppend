package com.upserve.uppend.lookup;

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

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(lookupDir);
    }

    @Test
    public void testCtor() throws Exception {
        new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
    }

    @Test
    public void testCtorErrors() throws Exception {
        Files.createDirectories(lookupDir);
        File notDir = File.createTempFile("not-a-dir", ".tmp", lookupDir.toFile());
        Path notDirPath = notDir.toPath();
        Exception expected = null;

        try {
            new LookupData(notDirPath.resolve("data"), notDirPath.resolve("meta"));
        } catch (UncheckedIOException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("can't open file"));

        expected = null;
        notDirPath = notDirPath.resolve("sub").resolve("sub2");
        try {
            new LookupData(notDirPath.resolve("data"), notDirPath.resolve("meta"));
        } catch (UncheckedIOException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("unable to make parent dir"));

        Files.write(lookupDir.resolve("data"), "short bad data".getBytes());
        new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));

        Files.write(lookupDir.resolve("data"), "bad data that is long enough to cross record".getBytes());
        expected = null;
        try {
            new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        } catch (ArrayIndexOutOfBoundsException e) {
            expected = e;
        }
        assertNotNull(expected);

        assertTrue(notDir.delete());
    }

    @Test
    public void testGetAndPut() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey");
        assertEquals(Long.MIN_VALUE, data.get(key));
        data.put(key, 80);
        assertEquals(80, data.get(key));
    }

    @Test
    public void testPutIfNotExists() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, 1);
        assertEquals(1, data.get(key));
        data.putIfNotExists(key, 2);
        assertEquals(1, data.get(key));
    }

    @Test
    public void testPutIfNotExistsFunction() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey");
        data.putIfNotExists(key, () -> 1);
        assertEquals(1, data.get(key));
        data.putIfNotExists(key, () -> 2);
        assertEquals(1, data.get(key));
    }

    @Test
    public void testFlushAndClose() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey");
        data.put(key, 80);
        data.flush();
        data.close();
        data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        assertEquals(80, data.get(key));
    }

    @Test
    public void testNumEntries() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        FileChannel dataChan = FileChannel.open(lookupDir.resolve("data"), StandardOpenOption.READ);
        assertEquals(0, LookupData.numEntries(dataChan));
        for (int i = 1; i <= 100; i++) {
            LookupKey key = new LookupKey(String.format("%05d", i));
            data.put(key, i);
            data.flush();
            assertEquals(i, LookupData.numEntries(dataChan));
        }
    }

    @Test
    public void testScan() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        data.put(new LookupKey("mykey1"), 1);
        data.put(new LookupKey("mykey2"), 2);
        data.flush();
        Map<String, Long> entries = new TreeMap<>();
        LookupData.scan(lookupDir.resolve("data"), entries::put);
        assertEquals(2, entries.size());
        assertArrayEquals(new String[] {"mykey1", "mykey2"}, entries.keySet().toArray(new String[0]));
        assertArrayEquals(new Long[] {1L, 2L}, entries.values().toArray(new Long[0]));
    }

    @Test
    public void testScanNonExistant() throws Exception {
        LookupData.scan(lookupDir.resolve("data"), (k, v) -> {
            throw new IllegalStateException("should not have called this");
        });
    }
}
