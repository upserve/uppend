package com.upserve.uppend.lookup;

import com.google.common.primitives.Ints;
import com.upserve.uppend.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LookupMetadataTest {

    private final String name = "lookupMetadata-test";
    private final Path rootPath = Paths.get("build/test/lookup/lookupMetadata");
    private final Path path = rootPath.resolve(name);
    private final Path keysPath = rootPath.resolve("lookupMetadataKeys-test");

    private VirtualPageFile virtualPageFile;
    private VirtualMutableBlobStore metadataBlobs;

    private static final int NUMBER_OF_STORES = 12;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private
    VirtualLongBlobStore mockLongBlobStore;

    @Mock
    private Logger mockLogger;

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);

        setup(false);
    }

    public void setup(boolean readOnly) {
        virtualPageFile = new VirtualPageFile(path, NUMBER_OF_STORES, 1024, readOnly);
        metadataBlobs = new VirtualMutableBlobStore(1, virtualPageFile);
    }

    @After
    public void after() throws IOException {
        // Just close - do not clear
        virtualPageFile.close();
    }

    @Test
    public void testCtorBadChecksum() throws Exception {
        buildSimpleTestData(metadataBlobs);
        // start a write in the middle of the valid data...
        metadataBlobs.write(12, Ints.toByteArray(2));

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Checksum did not match for the requested blob");

        LookupMetadata.open(metadataBlobs, 0);
    }

    @Test
    public void testInvalidContent() throws Exception {
        buildSimpleTestData(metadataBlobs);
        // start a write in the middle of the valid data...
        metadataBlobs.write(0, Ints.toByteArray(2));

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Meta blob is corrupted");

        LookupMetadata.open(metadataBlobs, 0);
    }


    @Test
    public void testCorrectReadWrite() throws Exception {
        buildSimpleTestData(metadataBlobs);

        LookupMetadata result = LookupMetadata.open(metadataBlobs, 3);
        assertArrayEquals(new int[]{0, 12}, result.getKeyStorageOrder());
        assertEquals(new LookupKey("b"), result.getMaxKey());
        assertEquals(new LookupKey("a"), result.getMinKey());
        assertEquals(3, result.getMetadataGeneration());
    }

    @Test
    public void testOpen() {
        LookupMetadata initialMetadata = LookupMetadata.open(metadataBlobs, 2);

        assertArrayEquals(new int[]{}, initialMetadata.getKeyStorageOrder());
        assertNull(initialMetadata.getMaxKey());
        assertNull(initialMetadata.getMinKey());
        assertEquals(2, initialMetadata.getMetadataGeneration());
    }

    @Test
    public void testOpenReadOnly() throws Exception {

        after(); // Close the writable store and reopen as read only
        setup(true);

        LookupMetadata initialMetadata = LookupMetadata.open(metadataBlobs, 2);

        assertArrayEquals(new int[]{}, initialMetadata.getKeyStorageOrder());
        assertNull(initialMetadata.getMaxKey());
        assertNull(initialMetadata.getMinKey());
        assertEquals(2, initialMetadata.getMetadataGeneration());
    }


    @Test
    public void testEmptyLookup() {
        LookupMetadata initialMetadata = new LookupMetadata(null, null, new int[0], 1);

        LookupKey searchKey = new LookupKey("Foo");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

    }

    @Test
    public void testOneKeyLookupAbove() {
        LookupKey oneKey = new LookupKey("Foo");
        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new int[]{0}, 1);

        LookupKey searchKey = new LookupKey("Bar");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

    }

    @Test
    public void testOneKeyLookupBelow() {
        LookupKey oneKey = new LookupKey("Foo");
        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new int[]{0}, 1);

        LookupKey searchKey = new LookupKey("Zar");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(0, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

    }

    @Test
    public void testOneKeyLookupEquals() {
        LookupKey oneKey = new LookupKey("Foo");
        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new int[]{0}, 1);

        Long expected = 5L;
        when(mockLongBlobStore.readLong(0)).thenReturn(expected);

        LookupKey searchKey = new LookupKey("Foo");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readLong(0);

        assertEquals(expected, result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(0, searchKey.getPosition());
    }

    @Test
    public void testTwoKeyLookupBelowLower() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new int[]{0, 1}, 1);

        LookupKey searchKey = new LookupKey("a");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

    }

    @Test
    public void testTwoKeyLookupEqualsLower() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new int[]{0, 1}, 1);

        Long expected = 5L;
        when(mockLongBlobStore.readLong(0)).thenReturn(expected);

        LookupKey searchKey = new LookupKey("b");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readLong(0);

        assertEquals(expected, result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(0, searchKey.getPosition());
    }

    @Test
    public void testTwoKeyLookupInBetween() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new int[]{0, 1}, 1);

        LookupKey searchKey = new LookupKey("m");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(0, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());
    }

    @Test
    public void testTwoKeyLookupEqualsUpper() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new int[]{0, 1}, 1);

        Long expected = 5L;
        when(mockLongBlobStore.readLong(1)).thenReturn(expected);

        LookupKey searchKey = new LookupKey("y");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readLong(1);

        assertEquals(expected, result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(1, searchKey.getPosition());
    }

    @Test
    public void testTwoKeyLookupEqualsUpperDifferentSortOrder() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new int[]{1, 0}, 1);

        Long expected = 5L;
        when(mockLongBlobStore.readLong(0)).thenReturn(expected);

        LookupKey searchKey = new LookupKey("y");
        Long result = initialMetadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readLong(0);

        assertEquals(expected, result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(0, searchKey.getPosition());
    }

    @Test
    public void testTwoKeyLookupAboveUpper() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new int[]{0, 1}, 1);


        LookupKey searchKey = new LookupKey("z");
        Long result = metadata.findKey(mockLongBlobStore, searchKey);

        verifyZeroInteractions(mockLongBlobStore);

        assertNull(result);
        assertEquals(1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());
    }

    @Test
    public void testManyKeysEqualsLastMidpoint() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new int[]{12, 7, 8, 1, 11, 6, 3, 5, 10, 2, 0, 4, 9}, 1);

        when(mockLongBlobStore.readBlob(3L)).thenReturn("m".getBytes()); // First midpoint is the 6th sort value => 3
        when(mockLongBlobStore.readBlob(2L)).thenReturn("s".getBytes()); // Second midpoint is the 9th sort value => 2
        Long expected = 5L;
        when(mockLongBlobStore.readLong(2)).thenReturn(expected);

        LookupKey searchKey = new LookupKey("s");
        Long result = metadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readBlob(3L);
        verify(mockLongBlobStore).readBlob(2L);
        verify(mockLongBlobStore).readLong(2);

        assertEquals(expected, result);
        assertEquals(-1, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(2, searchKey.getPosition());

        // These keys are now cached and don't require reading from the lookupData
        when(mockLongBlobStore.readLong(3)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("m")));
        verify(mockLongBlobStore).readLong(3);

        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("s")));
        verify(mockLongBlobStore, times(2)).readLong(2);

        verifyNoMoreInteractions(mockLongBlobStore);
    }

    @Test
    public void testManyKeysBelowLastMidpoint() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new int[]{12, 7, 8, 1, 11, 6, 3, 5, 10, 2, 0, 4, 9}, 1);

        when(mockLongBlobStore.readBlob(3L)).thenReturn("m".getBytes()); // First midpoint is the 6th sort value => 3
        when(mockLongBlobStore.readBlob(2L)).thenReturn("u".getBytes()); // Second midpoint is the 9th sort value => 2
        when(mockLongBlobStore.readBlob(5L)).thenReturn("o".getBytes()); // Third midpoint is the 7th sort value => 5
        when(mockLongBlobStore.readBlob(10L)).thenReturn("t".getBytes()); // Fourth midpoint is the 8th sort value => 5

        LookupKey searchKey = new LookupKey("s");
        Long result = metadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readBlob(3L);
        verify(mockLongBlobStore).readBlob(2L);
        verify(mockLongBlobStore).readBlob(5L);
        verify(mockLongBlobStore).readBlob(10L);

        assertNull(result);
        assertEquals(7, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

        // These keys are now cached and don't require reading from the lookupData, just getting the Long
        Long expected = 4L;
        when(mockLongBlobStore.readLong(3)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("m")));
        verify(mockLongBlobStore).readLong(3);

        when(mockLongBlobStore.readLong(2)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("u")));
        verify(mockLongBlobStore).readLong(2);

        when(mockLongBlobStore.readLong(5)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("o")));
        verify(mockLongBlobStore).readLong(5);

        when(mockLongBlobStore.readLong(10)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("t")));
        verify(mockLongBlobStore).readLong(10);

        verifyNoMoreInteractions(mockLongBlobStore);
    }

    @Test
    public void testManyKeysAboveLastMidpoint() {
        LookupKey bKey = new LookupKey("b");
        LookupKey yKey = new LookupKey("y");
        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new int[]{12, 7, 8, 1, 11, 6, 3, 5, 10, 2, 0, 4, 9}, 1);


        when(mockLongBlobStore.readBlob(3L)).thenReturn("m".getBytes()); // First midpoint is the 6th sort value => 3
        when(mockLongBlobStore.readBlob(2L)).thenReturn("u".getBytes()); // Second midpoint is the 9th sort value => 2
        when(mockLongBlobStore.readBlob(5L)).thenReturn("o".getBytes()); // Second midpoint is the 7th sort value => 5
        when(mockLongBlobStore.readBlob(10L)).thenReturn("q".getBytes()); // Second midpoint is the 8th sort value => 5

        LookupKey searchKey = new LookupKey("s");
        Long result = metadata.findKey(mockLongBlobStore, searchKey);

        verify(mockLongBlobStore).readBlob(3L);
        verify(mockLongBlobStore).readBlob(2L);
        verify(mockLongBlobStore).readBlob(5L);
        verify(mockLongBlobStore).readBlob(10L);

        assertNull(result);
        assertEquals(8, searchKey.getInsertAfterSortIndex());
        assertEquals(1, searchKey.getMetaDataGeneration());
        assertEquals(-1, searchKey.getPosition());

        // These keys are now cached and don't require reading from the lookupData, just getting the Long
        Long expected = 4L;
        when(mockLongBlobStore.readLong(3)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("m")));
        verify(mockLongBlobStore).readLong(3);

        when(mockLongBlobStore.readLong(2)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("u")));
        verify(mockLongBlobStore).readLong(2);

        when(mockLongBlobStore.readLong(5)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("o")));
        verify(mockLongBlobStore).readLong(5);

        when(mockLongBlobStore.readLong(10)).thenReturn(expected);
        assertEquals(expected, metadata.findKey(mockLongBlobStore, new LookupKey("q")));
        verify(mockLongBlobStore).readLong(10);

        verifyNoMoreInteractions(mockLongBlobStore);
    }


    @Test
    public void testMetadataLookup() {
        AppendOnlyStoreBuilder defaults = TestHelper.getDefaultAppendStoreTestBuilder()
                .withLookupPageSize(32 * 1024)
                .withMaximumLookupKeyCacheWeight(1024 * 1024);

        LookupCache lookupCache = defaults.buildLookupCache(name);

        PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);

        VirtualPageFile keysData = new VirtualPageFile(keysPath, NUMBER_OF_STORES, defaults.getLookupPageSize(), false);
        VirtualLongBlobStore keyStore = new VirtualLongBlobStore(5, keysData);

        LookupData lookupData = LookupData.lookupWriter(keyStore, metadataBlobs, partitionLookupCache, -1);
        List<Integer> keys = Ints.asList(IntStream.range(0, 4000).map(i -> i * 2).toArray());
        Collections.shuffle(keys, new Random(1234));
        keys.forEach(k -> lookupData.put(new LookupKey(String.valueOf(k)), 1000 + k));
        lookupData.flush();

        LookupMetadata metadata = LookupMetadata.open(lookupData.getMetadataBlobs(), 2);

        new Random()
                .ints(10_000, 0, 8000)
                .parallel()
                .forEach(key -> {
                            Long expected = null;
                            if (key % 2 == 0) expected = 1000L + key;
                            assertEquals(expected, metadata.findKey(keyStore, new LookupKey(String.valueOf(key))));
                        }
                );
    }

    @Test
    public void testToString() {
        LookupKey keyA = new LookupKey("00");
        LookupKey keyB = new LookupKey("01");
        LookupMetadata metadata = new LookupMetadata(keyA, keyB, new int[]{0, 1}, 4);
        String toString = metadata.toString();
        assertTrue(toString.contains("numKeys=2"));
        assertTrue(toString.contains("minKey=00"));
        assertTrue(toString.contains("maxKey=01"));
    }

    @Test
    public void testFindKeyLogging() throws Exception {
        LookupKey key1 = new LookupKey("key1");
        LookupKey key2 = new LookupKey("key2");
        LookupKey key3 = new LookupKey("key3");
        LookupMetadata initialMetadata = new LookupMetadata(key1, key3, new int[] {0, 1, 2}, 1);

        when(mockLongBlobStore.readBlob(1L)).thenReturn("key2".getBytes());

        when(mockLogger.isTraceEnabled()).thenReturn(true);

        TestHelper.setLogger(LookupMetadata.class, "log", mockLogger);
        try {
            initialMetadata.findKey(mockLongBlobStore, key2);
        } finally {
            TestHelper.resetLogger(LookupMetadata.class, "log");
        }

        verify(mockLogger).isTraceEnabled();
        verify(mockLogger).trace("reading {}: [{}, {}], [{}, {}], {}", key2, 0, 2, key1, key3, 1);
        verifyNoMoreInteractions(mockLogger);
    }

    private void buildSimpleTestData(VirtualMutableBlobStore blobStore) throws IOException {
        LookupKey keyA = new LookupKey("a");
        LookupKey keyB = new LookupKey("b");
        LookupMetadata metadata = new LookupMetadata(keyA, keyB, new int[]{0, 12}, 0);
        Files.createDirectories(path.getParent());
        metadata.writeTo(blobStore);
    }
}
