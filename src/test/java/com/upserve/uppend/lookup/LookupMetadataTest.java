package com.upserve.uppend.lookup;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.*;

public class LookupMetadataTest {
    @Test
    public void testSearchMidpointPercentageFunction() {
        assertEquals(48, LookupMetadata.searchMidpointPercentage("aa", "zyzzyva", "middle"));
    }

    @Test
    public void testSearchMidpointPercentageFunctionEdgeCases() {
        assertEquals(50, LookupMetadata.searchMidpointPercentage("zyzzyva", "aa", "middle"));
        assertEquals(50, LookupMetadata.searchMidpointPercentage("aa", "be", "oi"));
    }

    @Test
    public void testMetadataLookup() throws IOException {
        Path dataPath = Paths.get("build/test/lookup-metadata-test/test-foo/data");
        Files.deleteIfExists(dataPath);
        Path metadataPath = dataPath.resolveSibling("meta");
        LookupData lookupData = new LookupData(2, dataPath, metadataPath);
        lookupData.put(new LookupKey("00"), 1000);
        lookupData.put(new LookupKey("01"), 1001);
        lookupData.put(new LookupKey("02"), 1002);
        lookupData.put(new LookupKey("03"), 1003);
        lookupData.put(new LookupKey("04"), 1004);
        lookupData.put(new LookupKey("05"), 1005);
        lookupData.put(new LookupKey("06"), 1006);
        lookupData.put(new LookupKey("07"), 1007);
        lookupData.put(new LookupKey("08"), 1008);
        lookupData.put(new LookupKey("09"), 1009);
        lookupData.close();
        LookupMetadata metadata = new LookupMetadata(metadataPath);
        assertEquals(1000, metadata.readData(dataPath, new LookupKey("00")));
        assertEquals(1001, metadata.readData(dataPath, new LookupKey("01")));
        assertEquals(1002, metadata.readData(dataPath, new LookupKey("02")));
        assertEquals(1003, metadata.readData(dataPath, new LookupKey("03")));
        assertEquals(1004, metadata.readData(dataPath, new LookupKey("04")));
        assertEquals(1005, metadata.readData(dataPath, new LookupKey("05")));
        assertEquals(1006, metadata.readData(dataPath, new LookupKey("06")));
        assertEquals(1007, metadata.readData(dataPath, new LookupKey("07")));
        assertEquals(1008, metadata.readData(dataPath, new LookupKey("08")));
        assertEquals(1009, metadata.readData(dataPath, new LookupKey("09")));
    }
}
