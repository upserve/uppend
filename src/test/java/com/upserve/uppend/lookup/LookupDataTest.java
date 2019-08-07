package com.upserve.uppend.lookup;

import com.google.common.primitives.Ints;
import com.upserve.uppend.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
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

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Check checksum
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Checksum did not match for the requested blob");

        LookupData data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
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

    @Test
    public void testGetMetadataShouldNotLoadMetada_1() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);
        data.timeStampedMetadata.set(expected, 5);
        LookupMetadata lmd1 = data.getMetadata();
        assertTrue(expected == lmd1);
        Mockito.verify(data, never()).loadMetadata();
    }

    @Test
    // The reload interval is set to 5s but the data is set to be reloaded at 10s, so the reload will
    // not happen because not enough actual time has elapsed (this test runs in mere milliseconds).
    public void testGetMetadataShouldNotLoadMetada_2() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 5));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);
        data.timeStampedMetadata.set(expected, 10);
        LookupMetadata lmd1 = data.getMetadata();
        assertTrue(expected ==  lmd1);
        Mockito.verify(data, never()).loadMetadata();
    }

    @Test
    // The reload interval is set to 20s and the last reload time is set to -10s (an absolutely fake time)
    // in order to force the loadMetadata method to be called.
    public void testGetMetadataShouldLoadMetada_1() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 20));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);
        data.reloadStamp.set(-10);
        data.timeStampedMetadata.set(expected, -10);
        LookupMetadata lmd1 = data.getMetadata();
        assertFalse("with no data in the mutableBlobStore, getMetadata returns a new instance",
                expected == lmd1);
        Mockito.verify(data, times(1)).loadMetadata(any());
    }

    @Test
    public void testGetMetadataShouldLoadMetada_2() {
        LookupData dataWriter = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        // add a key & value to the blob store
        final LookupKey key1 = new LookupKey("mykey1");
        dataWriter.put(key1, 80);
        dataWriter.flush();

        LookupData dataReader = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 20));
        int[] stamp = new int[1];
        LookupMetadata expected = dataReader.timeStampedMetadata.get(stamp);
        dataReader.reloadStamp.set(-10);
        dataReader.timeStampedMetadata.set(expected, -10);
        LookupMetadata lmd1 = dataReader.getMetadata();

        assertTrue("with data in the mutableBlobStore, getMetadata should not return a new instance",
                expected == lmd1);
        assertEquals(lmd1.getNumKeys(), 1);
        Mockito.verify(dataReader, times(1)).loadMetadata(any());

        // add a second key & value to the blob store
        final LookupKey key2 = new LookupKey("mykey2");
        dataWriter.put(key2, 80);
        dataWriter.flush();

        dataReader.reloadStamp.set(-10);
        dataReader.timeStampedMetadata.set(lmd1, -10);
        LookupMetadata lmd2 = dataReader.getMetadata();

        assertTrue("a new key has been added, so the LookupMetadata instance should be new",
                lmd2 != lmd1);
        assertEquals(lmd2.getNumKeys(), 2);
        Mockito.verify(dataReader, times(2)).loadMetadata(any());

        LookupMetadata lmd3 = dataReader.getMetadata();
        assertTrue("nothing has changed since the last call to getMetadata so the instance should not change",
                lmd2 == lmd3);
    }
}
