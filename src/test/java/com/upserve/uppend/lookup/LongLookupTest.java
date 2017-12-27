package com.upserve.uppend.lookup;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;

public class LongLookupTest {
    @Test
    public void testCtorErrors() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);

        Exception expected = null;
        try {
            new LongLookup(path, 0, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        } catch (IllegalArgumentException e) {
            expected = e;
        }
        assertNotNull(expected);

        expected = null;
        try {
            new LongLookup(path, (1 << 24) + 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        } catch (IllegalArgumentException e) {
            expected = e;
        }
        assertNotNull(expected);

        expected = null;
        try {
            new LongLookup(path, 1, -1);
        } catch (IllegalArgumentException e) {
            expected = e;
        }
        assertNotNull(expected);
    }

    @Test
    public void testKeysDepth1() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth2() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 257, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth3() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 65537, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testPartitions() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path);
        longLookup.put("b", "b", 1);
        longLookup.put("c", "c", 1);
        longLookup.put("a", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.partitions().sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testGetFlushed() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path);
        longLookup.put("a", "b", 1);
        LongLookup longLookup2 = new LongLookup(path);
        assertEquals(-1, longLookup2.getFlushed("a", "b"));
        longLookup.flush();
        assertEquals(1, longLookup2.getFlushed("a", "b"));
        longLookup2.close();
        longLookup.close();
    }


    @Test
    public void testScan() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path);
        longLookup.put("a", "a1", 1);
        longLookup.put("a", "a2", 2);
        longLookup.put("b", "b1", 1);
        longLookup.put("b", "b2", 2);
        longLookup.close();

        longLookup = new LongLookup(path);
        Map<String, Long> results = new TreeMap<>();
        longLookup.scan("a", results::put);
        assertArrayEquals(new String[] {"a1", "a2"}, results.keySet().toArray(new String[0]));
        assertArrayEquals(new Long[] {1L, 2L}, results.values().toArray(new Long[0]));

        results.clear();
        longLookup.scan("b", results::put);
        assertArrayEquals(new String[] {"b1", "b2"}, results.keySet().toArray(new String[0]));
        assertArrayEquals(new Long[] {1L, 2L}, results.values().toArray(new Long[0]));

        longLookup.scan("c", (k, v) -> { throw new IllegalStateException("should not have been called"); });

        longLookup.close();
    }
}
