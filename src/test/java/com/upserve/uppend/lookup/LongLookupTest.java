package com.upserve.uppend.lookup;

import com.google.common.hash.HashCode;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class LongLookupTest {
    private final Path path = Paths.get("build/test/long-lookup-test");

    @Before
    public void init() throws IOException {
        SafeDeleting.removeDirectory(path);
    }

    @Test
    public void testCtorErrors() throws Exception {
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
    public void testHashPath() throws IOException {
        for (int hashSize : new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 }) {
            Path hashPath = path.resolve(String.format("byte-boundary-%d", hashSize));
            SafeDeleting.removeDirectory(hashPath);
            try (LongLookup longLookup = new LongLookup(hashPath, hashSize, 1)) {
                Map<String, Long> hashMap = IntStream
                        .range(0, 1_048_576)
                        .mapToObj(HashCode::fromInt)
                        .map(longLookup::hashPath)
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
                assertEquals(hashSize, hashMap.size());
            }
        }
    }

    @Test
    public void testKeysDepth1() throws Exception {
        LongLookup longLookup = new LongLookup(path, 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth2() throws Exception {
        LongLookup longLookup = new LongLookup(path, 257, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth3() throws Exception {
        LongLookup longLookup = new LongLookup(path, 65537, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testPartitions() throws Exception {
        LongLookup longLookup = new LongLookup(path);
        longLookup.put("b", "b", 1);
        longLookup.put("c", "c", 1);
        longLookup.put("a", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.partitions().sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testGetFlushed() throws Exception {
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
