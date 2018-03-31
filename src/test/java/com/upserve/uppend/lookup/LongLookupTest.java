package com.upserve.uppend.lookup;

import com.google.common.hash.HashCode;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

import static org.junit.Assert.*;

public class LongLookupTest {
    private final Path path = Paths.get("build/test/long-lookup-test");

    private final LookupCache lookupCache = new LookupCache();

    @Before
    public void init() throws IOException {
        SafeDeleting.removeDirectory(path);
    }

    @Test
    public void testCtorErrors() throws Exception {
        Exception expected = null;
        try {
            new LongLookup(path, lookupCache, 0, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        } catch (IllegalArgumentException e) {
            expected = e;
        }
        assertNotNull(expected);

        expected = null;
        try {
            new LongLookup(path, lookupCache,(1 << 24) + 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        } catch (IllegalArgumentException e) {
            expected = e;
        }
        assertNotNull(expected);

        expected = null;
        try {
            new LongLookup(path, lookupCache,  1, -1);
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
            try (LongLookup longLookup = new LongLookup(hashPath, lookupCache, hashSize, 1)) {
                assertEquals(hashSize, IntStream
                        .range(0, hashSize)
                        .mapToObj(HashCode::fromInt)
                        .map(longLookup::hashPath)
                        .distinct()
                        .count()
                );
            }
        }
    }

    @Test
    public void testKeysDepth1() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache, 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth2() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache, 257, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth3() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache, 65537, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testPartitions() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache);
        longLookup.put("b", "b", 1);
        longLookup.put("c", "c", 1);
        longLookup.put("a", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.partitions().sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testGetFlushed() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache);
        longLookup.put("a", "b", 1);
        LongLookup longLookup2 = new LongLookup(path, lookupCache);
        assertEquals(-1, longLookup2.getFlushed("a", "b"));
        longLookup.flush();
        assertEquals(1, longLookup2.getFlushed("a", "b"));
        longLookup2.close();
        longLookup.close();
    }


    @Test
    public void testScan() throws Exception {
        LongLookup longLookup = new LongLookup(path, lookupCache);
        longLookup.put("a", "a1", 1);
        longLookup.put("a", "a2", 2);
        longLookup.put("b", "b1", 1);
        longLookup.put("b", "b2", 2);
        longLookup.close();

        longLookup = new LongLookup(path, lookupCache);
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

    @Test
    public void testPutIfNotExisting() {
        long putCount = 500_000L;
        int hashSize = 32;
        int writeCacheSize = 900;
        // Actual number of hashPaths will be hashSize * 36 -> 1152
        int entropy = 3; // number of alpha numeric characters to use as entropy

        LongLookup longLookup = new LongLookup(path, lookupCache, hashSize, writeCacheSize);

        AtomicInteger counter = new AtomicInteger();
        LongSupplier supplier = () -> {
            counter.getAndAdd(1);
            return ThreadLocalRandom.current().nextLong();
        };

        Map<String, Long> result = IntStream.range(0, (int) putCount)
                .parallel()
                .mapToObj(i -> randomString(entropy))
                .peek(s -> longLookup.putIfNotExists(s.substring(0,1), s, supplier))
                .collect(
                        Collectors.groupingByConcurrent(Function.identity(),
                                Collectors.counting())
                );

        // The size of the map should equal the number of time the supplier was called by the put operation
        assertEquals(result.size(), counter.get());
        // The sum of the counts for each key should equal the number of time put was called
        assertEquals(putCount, result.values().stream().mapToLong(val -> val).sum());
        longLookup.close();
    }

    private static final int ALPHA_NUMS_LEN = 26 + 10;
    private static char[] ALPHA_NUMS = new char[ALPHA_NUMS_LEN];
    static {
        int i = 0;
        for (char c = 'a'; c <= 'z'; c++) {
            ALPHA_NUMS[i++] = c;
        }
        for (char c = '0'; c <= '9'; c++) {
            ALPHA_NUMS[i++] = c;
        }
    }

    private static final Random ALPHA_NUM_RANDOMNESS = new Random(1234);

    private static String randomString(int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = ALPHA_NUMS[ALPHA_NUM_RANDOMNESS.nextInt(ALPHA_NUMS_LEN)];
        }
        return new String(chars);
    }
}
