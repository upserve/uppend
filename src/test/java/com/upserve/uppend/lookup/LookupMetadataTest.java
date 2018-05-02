package com.upserve.uppend.lookup;

import com.google.common.primitives.Ints;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    @Mock
    VirtualLongBlobStore mockLongBlobStore;

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
        virtualPageFile.close();
    }

    @Test
    public void testCtorCorruptOrder() throws Exception{
        buildSimpleTestData(metadataBlobs);

        metadataBlobs.write(0, Ints.toByteArray(2));

        Exception expected = null;
        try {
            LookupMetadata.open(metadataBlobs, 0);
        } catch (IllegalStateException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("Meta blob is corrupted"));
    }

    @Test
    public void testCorrectReadWrite() throws Exception{
        buildSimpleTestData(metadataBlobs);

        LookupMetadata result = LookupMetadata.open(metadataBlobs, 3);
        assertArrayEquals(new long[]{0,12}, result.getKeyStorageOrder());
        assertEquals(new LookupKey("b"), result.getMaxKey());
        assertEquals(new LookupKey("a"), result.getMinKey());
        assertEquals(3, result.getMetadataGeneration());
    }

    @Test
    public void testOpen() throws Exception{
        LookupMetadata initialMetadata = LookupMetadata.open(metadataBlobs, 2);

        assertArrayEquals(new long[]{}, initialMetadata.getKeyStorageOrder());
        assertNull(initialMetadata.getMaxKey());
        assertNull(initialMetadata.getMinKey());
        assertEquals(2, initialMetadata.getMetadataGeneration());
    }

    @Test
    public void testOpenReadOnly() throws Exception{

        after(); // Close the writable store and reopen as read only
        setup(true);

        LookupMetadata initialMetadata = LookupMetadata.open(metadataBlobs, 2);

        assertArrayEquals(new long[]{}, initialMetadata.getKeyStorageOrder());
        assertNull(initialMetadata.getMaxKey());
        assertNull(initialMetadata.getMinKey());
        assertEquals(2, initialMetadata.getMetadataGeneration());
    }


//
//    @Test
//    public void testEmptyLookup(){
//        LookupMetadata initialMetadata = new LookupMetadata(null, null, new long[0], 1);
//
//        LookupKey searchKey = new LookupKey("Foo");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verifyZeroInteractions(mockLookupData);
//
//        assertEquals(null, result);
//        assertEquals(-1, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testOneKeyLookupAbove(){
//        LookupKey oneKey = new LookupKey("Foo");
//        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new long[]{0}, 1);
//
//        LookupKey searchKey = new LookupKey("Bar");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verifyZeroInteractions(mockLookupData);
//
//        assertEquals(null, result);
//        assertEquals(-1, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testOneKeyLookupBelow(){
//        LookupKey oneKey = new LookupKey("Foo");
//        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new long[]{0}, 1);
//
//        LookupKey searchKey = new LookupKey("Zar");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verifyZeroInteractions(mockLookupData);
//
//        assertEquals(null, result);
//        assertEquals(0, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testOneKeyLookupEquals(){
//        LookupKey oneKey = new LookupKey("Foo");
//        LookupMetadata initialMetadata = new LookupMetadata(oneKey, oneKey, new long[]{0}, 1);
//
//        Long expected = 5L;
//        when(mockLookupData.readValue(0)).thenReturn(expected);
//        verifyNoMoreInteractions(mockLookupData);
//
//        LookupKey searchKey = new LookupKey("Foo");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        assertEquals(expected, result);
//        assertEquals(0, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testTwoKeyLookupBelowLower(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new long[]{0,1}, 1);
//
//        LookupKey searchKey = new LookupKey("a");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verifyZeroInteractions(mockLookupData);
//
//        assertEquals(null, result);
//        assertEquals(-1, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testTwoKeyLookupEqualsLower(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new long[]{0,1}, 1);
//
//        Long expected = 5L;
//        when(mockLookupData.readValue(0)).thenReturn(expected);
//
//        LookupKey searchKey = new LookupKey("b");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readValue(0);
//
//        assertEquals(expected, result);
//        assertEquals(0, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testTwoKeyLookupInBetween(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new long[]{0,1}, 1);
//
//        LookupKey searchKey = new LookupKey("m");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verifyZeroInteractions(mockLookupData);
//
//        assertEquals(null, result);
//        assertEquals(0, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testTwoKeyLookupEqualsUpper(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new long[]{0,1}, 1);
//
//        Long expected = 5L;
//        when(mockLookupData.readValue(1)).thenReturn(expected);
//
//        LookupKey searchKey = new LookupKey("y");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readValue(1);
//
//        assertEquals(expected, result);
//        assertEquals(1, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//    }
//
//    @Test
//    public void testTwoKeyLookupEqualsUpperDifferentSortOrder(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata initialMetadata = new LookupMetadata(bKey, yKey, new long[]{1,0}, 1);
//
//        Long expected = 5L;
//        when(mockLookupData.readValue(0)).thenReturn(expected);
//
//        LookupKey searchKey = new LookupKey("y");
//        Long result = initialMetadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readValue(0);
//
//        assertEquals(expected, result);
//        assertEquals(0, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//
//    }
//
//    @Test
//    public void testTwoKeyLookupAboveUpper(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new long[]{0,1}, 1);
//
//
//        LookupKey searchKey = new LookupKey("z");
//        Long result = metadata.findKey(mockLookupData, searchKey);
//
//        assertEquals(null, result);
//        assertEquals(1, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//
//        verifyZeroInteractions(mockLookupData);
//    }
//
//    @Test
//    public void testManyKeysEqualsLastMidpoint(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new long[]{12,7,8,1,11,6,3,5,10,2,0,4,9}, 1);
//
//        when(mockLookupData.readKey(3L)).thenReturn(new LookupKey("m")); // First midpoint is the 6th sort value => 3
//        when(mockLookupData.readKey(2L)).thenReturn(new LookupKey("s")); // Second midpoint is the 9th sort value => 2
//        Long expected = 5L;
//        when(mockLookupData.readValue(2)).thenReturn(expected);
//
//        LookupKey searchKey = new LookupKey("s");
//        Long result = metadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readKey(3L);
//        verify(mockLookupData).readKey(2L);
//        verify(mockLookupData).readValue(2);
//
//        assertEquals(expected, result);
//        assertEquals(2, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//
//        // These keys are now cached and don't require reading from the lookupData
//        when(mockLookupData.readValue(3)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("m")));
//        verify(mockLookupData).readValue(3);
//
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("s")));
//        verify(mockLookupData, times(2)).readValue(2);
//
//        verifyNoMoreInteractions(mockLookupData);
//    }
//
//    @Test
//    public void testManyKeysBelowLastMidpoint(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new long[]{12,7,8,1,11,6,3,5,10,2,0,4,9}, 1);
//
//        when(mockLookupData.readKey(3L)).thenReturn(new LookupKey("m")); // First midpoint is the 6th sort value => 3
//        when(mockLookupData.readKey(2L)).thenReturn(new LookupKey("u")); // Second midpoint is the 9th sort value => 2
//        when(mockLookupData.readKey(5L)).thenReturn(new LookupKey("o")); // Third midpoint is the 7th sort value => 5
//        when(mockLookupData.readKey(10L)).thenReturn(new LookupKey("t")); // Fourth midpoint is the 8th sort value => 5
//
//        LookupKey searchKey = new LookupKey("s");
//        Long result = metadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readKey(3L);
//        verify(mockLookupData).readKey(2L);
//        verify(mockLookupData).readKey(5L);
//        verify(mockLookupData).readKey(10L);
//
//        assertEquals(null, result);
//        assertEquals(5, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//
//        // These keys are now cached and don't require reading from the lookupData, just getting the Long
//        Long expected = 4L;
//        when(mockLookupData.readValue(3)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("m")));
//        verify(mockLookupData).readValue(3);
//
//        when(mockLookupData.readValue(2)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("u")));
//        verify(mockLookupData).readValue(2);
//
//        when(mockLookupData.readValue(5)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("o")));
//        verify(mockLookupData).readValue(5);
//
//        when(mockLookupData.readValue(10)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("t")));
//        verify(mockLookupData).readValue(10);
//
//        verifyNoMoreInteractions(mockLookupData);
//    }
//
//    @Test
//    public void testManyKeysAboveLastMidpoint(){
//        LookupKey bKey = new LookupKey("b");
//        LookupKey yKey = new LookupKey("y");
//        LookupMetadata metadata = new LookupMetadata(bKey, yKey, new long[]{12,7,8,1,11,6,3,5,10,2,0,4,9}, 1);
//
//
//        when(mockLookupData.readKey(3L)).thenReturn(new LookupKey("m")); // First midpoint is the 6th sort value => 3
//        when(mockLookupData.readKey(2L)).thenReturn(new LookupKey("u")); // Second midpoint is the 9th sort value => 2
//        when(mockLookupData.readKey(5L)).thenReturn(new LookupKey("o")); // Second midpoint is the 7th sort value => 5
//        when(mockLookupData.readKey(10L)).thenReturn(new LookupKey("q")); // Second midpoint is the 8th sort value => 5
//
//        LookupKey searchKey = new LookupKey("s");
//        Long result = metadata.findKey(mockLookupData, searchKey);
//
//        verify(mockLookupData).readKey(3L);
//        verify(mockLookupData).readKey(2L);
//        verify(mockLookupData).readKey(5L);
//        verify(mockLookupData).readKey(10L);
//
//        assertEquals(null, result);
//        assertEquals(10, searchKey.getInsertAfterSortIndex());
//        assertEquals(1, searchKey.getMetaDataGeneration());
//
//        // These keys are now cached and don't require reading from the lookupData, just getting the Long
//        Long expected = 4L;
//        when(mockLookupData.readValue(3)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("m")));
//        verify(mockLookupData).readValue(3);
//
//        when(mockLookupData.readValue(2)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("u")));
//        verify(mockLookupData).readValue(2);
//
//        when(mockLookupData.readValue(5)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("o")));
//        verify(mockLookupData).readValue(5);
//
//        when(mockLookupData.readValue(10)).thenReturn(expected);
//        assertEquals(expected, metadata.findKey(mockLookupData, new LookupKey("q")));
//        verify(mockLookupData).readValue(10);
//
//        verifyNoMoreInteractions(mockLookupData);
//    }
//
//
//    @Test
//    public void testMetadataLookup() throws IOException {
//        AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder()
//            .withLookupPageSize(32*1024)
//            .withMaximumLookupKeyCacheWeight(1024 * 1024);
//
//        PageCache pageCache = defaults.buildLookupPageCache(name);
//        LookupCache lookupCache = defaults.buildLookupCache(name);
//
//        PartitionLookupCache partitionLookupCache = PartitionLookupCache.create("partition", lookupCache);
//
//        VirtualPageFile keysData = new VirtualPageFile(keysPath, NUMBER_OF_STORES, PAGE_SIZE, false);
//        VirtualLongBlobStore keyStore = new VirtualLongBlobStore(5, keysData);
//
//        LookupData lookupData = new LookupData(keyStore, metadataBlobs, partitionLookupCache, false);
//        List<Integer> keys = Ints.asList(IntStream.range(0, 4000).map(i -> i*2).toArray());
//        Collections.shuffle(keys, new Random(1234));
//        keys.forEach(k -> lookupData.put(new LookupKey(String.valueOf(k)), 1000 + k));
//        lookupData.flush();
//
//        LookupMetadata metadata = new LookupMetadata(lookupData.getMetadataPath(), 1);
//
//        new Random()
//                .ints(10_000, 0, 8000)
//                .parallel()
//                .forEach(key -> {
//                            Long expected = null;
//                            if (key % 2 == 0) expected = 1000L + key;
//                            assertEquals(expected, metadata.findKey(lookupData, new LookupKey(String.valueOf(key))));
//                        }
//                );
//    }
//
//    @Test
//    public void testToString() throws Exception {
//        LookupKey keyA = new LookupKey("00");
//        LookupKey keyB = new LookupKey("01");
//        LookupMetadata metadata = new LookupMetadata(keyA, keyB, new long[]{0,1},4);
//        String toString = metadata.toString();
//        assertTrue(toString.contains("numKeys=2"));
//        assertTrue(toString.contains("minKey=00"));
//        assertTrue(toString.contains("maxKey=01"));
//    }
//
//    private ByteBuffer bufferOf(int val) {
//        ByteBuffer buffer = ByteBuffer.allocate(4);
//        buffer.putInt(val);
//        buffer.flip();
//        return buffer;
//    }
//
    private void buildSimpleTestData(VirtualMutableBlobStore blobStore) throws IOException {
        LookupKey keyA = new LookupKey("a");
        LookupKey keyB = new LookupKey("b");
        LookupMetadata metadata = new LookupMetadata(keyA, keyB, new long[] {0, 12}, 0);
        Files.createDirectories(path.getParent());
        metadata.writeTo(blobStore);
    }

}
