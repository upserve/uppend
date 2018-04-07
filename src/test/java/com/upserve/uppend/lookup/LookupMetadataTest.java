package com.upserve.uppend.lookup;

import com.google.common.primitives.Ints;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class LookupMetadataTest {
    @Test
    public void testCtorCorruptOrder() throws Exception{
        Path path = Paths.get("build/test/lookup-metadata-test/testCtorCorruptOrder/meta");

        LookupKey keyA = new LookupKey("a");
        LookupKey keyB = new LookupKey("b");
        LookupMetadata metadata = new LookupMetadata(2, keyA, keyB, new int[] {0}, 0);
        Files.createDirectories(path.getParent());
        metadata.writeTo(path);

        Exception expected = null;
        try {
            new LookupMetadata(path, 0);
        } catch (IllegalStateException e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("expected 2 keys, got 1"));
    }


//    @Test
//    public void testMetadataLookup() throws IOException {
//        Path dataPath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup/data");
//        Files.deleteIfExists(dataPath);
//        Path metadataPath = dataPath.resolveSibling("meta");
//        LookupData lookupData = new LookupData(dataPath, metadataPath);
//        lookupData.put(new LookupKey("00"), 1000);
//        lookupData.put(new LookupKey("01"), 1001);
//        lookupData.put(new LookupKey("02"), 1002);
//        lookupData.put(new LookupKey("03"), 1003);
//        lookupData.put(new LookupKey("04"), 1004);
//        lookupData.put(new LookupKey("05"), 1005);
//        lookupData.put(new LookupKey("06"), 1006);
//        lookupData.put(new LookupKey("07"), 1007);
//        lookupData.put(new LookupKey("08"), 1008);
//        lookupData.put(new LookupKey("09"), 1009);
//        lookupData.close();
//        LookupMetadata metadata = new LookupMetadata(metadataPath);
//        assertEquals(1000, metadata.readData(dataPath, new LookupKey("00")));
//        assertEquals(1001, metadata.readData(dataPath, new LookupKey("01")));
//        assertEquals(1002, metadata.readData(dataPath, new LookupKey("02")));
//        assertEquals(1003, metadata.readData(dataPath, new LookupKey("03")));
//        assertEquals(1004, metadata.readData(dataPath, new LookupKey("04")));
//        assertEquals(1005, metadata.readData(dataPath, new LookupKey("05")));
//        assertEquals(1006, metadata.readData(dataPath, new LookupKey("06")));
//        assertEquals(1007, metadata.readData(dataPath, new LookupKey("07")));
//        assertEquals(1008, metadata.readData(dataPath, new LookupKey("08")));
//        assertEquals(1009, metadata.readData(dataPath, new LookupKey("09")));
//    }
//
//    @Test
//    public void testMetadataLookup2() throws IOException {
//        Path storePath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup");
//        SafeDeleting.removeDirectory(storePath);
//        Path dataPath = storePath.resolve("data");
//        Path metadataPath = storePath.resolve("meta");
//        LookupData lookupData = new LookupData(dataPath, metadataPath);
//        lookupData.put(new LookupKey("412"), 1000);
//        lookupData.put(new LookupKey("3036"), 1001);
//        lookupData.put(new LookupKey("7132"), 1002);
//        lookupData.close();
//        LookupMetadata metadata = new LookupMetadata(metadataPath);
//        assertEquals(1000, metadata.readData(dataPath, new LookupKey("412")));
//        assertEquals(1001, metadata.readData(dataPath, new LookupKey("3036")));
//        assertEquals(1002, metadata.readData(dataPath, new LookupKey("7132")));
//    }
//
//    @Test
//    public void testMetadataLookup3() throws IOException {
//        Path storePath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup");
//        SafeDeleting.removeDirectory(storePath);
//        Path dataPath = storePath.resolve("data");
//        Path metadataPath = storePath.resolve("meta");
//        LookupData lookupData = new LookupData(dataPath, metadataPath);
//        lookupData.put(new LookupKey("00"), 1000);
//        lookupData.put(new LookupKey("01"), 1001);
//        lookupData.put(new LookupKey("02"), 1002);
//        lookupData.put(new LookupKey("03"), 1003);
//        lookupData.put(new LookupKey("04"), 1004);
//        lookupData.put(new LookupKey("05"), 1005);
//        lookupData.put(new LookupKey("06"), 1006);
//        lookupData.put(new LookupKey("07"), 1007);
//        lookupData.put(new LookupKey("08"), 1008);
//        lookupData.put(new LookupKey("09"), 1009);
//        lookupData.put(new LookupKey("010"), 1010);
//        lookupData.close();
//        LookupMetadata metadata = new LookupMetadata(metadataPath);
//        assertEquals(1000, metadata.readData(dataPath, new LookupKey("00")));
//        assertEquals(1001, metadata.readData(dataPath, new LookupKey("01")));
//        assertEquals(1002, metadata.readData(dataPath, new LookupKey("02")));
//        assertEquals(1003, metadata.readData(dataPath, new LookupKey("03")));
//        assertEquals(1004, metadata.readData(dataPath, new LookupKey("04")));
//        assertEquals(1005, metadata.readData(dataPath, new LookupKey("05")));
//        assertEquals(1006, metadata.readData(dataPath, new LookupKey("06")));
//        assertEquals(1007, metadata.readData(dataPath, new LookupKey("07")));
//        assertEquals(1008, metadata.readData(dataPath, new LookupKey("08")));
//        assertEquals(1009, metadata.readData(dataPath, new LookupKey("09")));
//        assertEquals(1010, metadata.readData(dataPath, new LookupKey("010")));
//    }
//
//    @Test
//    public void testMetadataLookup4() throws IOException {
//        Path storePath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup");
//        SafeDeleting.removeDirectory(storePath);
//        Path dataPath = storePath.resolve("data");
//        Path metadataPath = storePath.resolve("meta");
//        LookupData lookupData = new LookupData(dataPath, metadataPath);
//        List<Integer> keys = Ints.asList(IntStream.range(0, 1000).toArray());
//        Collections.shuffle(keys, new Random(1234));
//        keys.forEach(k -> lookupData.put(new LookupKey(String.valueOf(k)), 1000 + k));
//        lookupData.close();
//        LookupMetadata metadata = new LookupMetadata(metadataPath);
//        keys.sort(Integer::compareTo);
//        for (Integer key : keys) {
//            assertEquals(1000 + key, metadata.readData(dataPath, new LookupKey(String.valueOf(key))));
//        }
//    }
//
//    @Test
//    public void testToString() throws Exception {
//        Path storePath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup");
//        SafeDeleting.removeDirectory(storePath);
//        Path dataPath = storePath.resolve("data");
//        Path metadataPath = storePath.resolve("meta");
//        LookupData lookupData = new LookupData(dataPath, metadataPath);
//        lookupData.put(new LookupKey("00"), 1000);
//        lookupData.put(new LookupKey("01"), 1001);
//        lookupData.close();
//        LookupMetadata metadata = new LookupMetadata(metadataPath);
//        String toString = metadata.toString();
//        assertTrue(toString.contains("numKeys=2"));
//        assertTrue(toString.contains("minKey=00"));
//        assertTrue(toString.contains("maxKey=01"));
//    }
}
