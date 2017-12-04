package com.upserve.uppend.lookup;

import com.upserve.uppend.util.Throwables;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
        Path dataPath = Paths.get("build/test/lookup-metadata-test/testMetadataLookup/data");
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

    @Test
    public void testTruncatedDataFile() throws Exception {
        Path dir = Paths.get("build/test/lookup-metadata-test/testTruncatedDataFile");
        Files.createDirectories(dir);

        Path dataPath = dir.resolve("data");
        FileChannel dataChan = FileChannel.open(dataPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        /* data:
        00000000  62 62 62 62 62 62 62 62  2d 62 62 62 62 62 62 62  |bbbbbbbb-bbbbbbb|
        00000010  2d 62 62 62 62 2d 62 62  62 62 62 62 62 2d 62 62  |-bbbb-bbbbbbb-bb|
        00000020  62 62 3a 3a 62 62 62 62  62 62 62 00 00 00 00 00  |bb::bbbbbbb.....|
        00000030  00 00 05 63 63 63 63 63  63 63 2d 63 63 63 63 63  |...ccccccc-ccccc|
        00000040  63 63 63 63 63 2d 63 63  63 63 63 63 63 2d 63 63  |ccccc-ccccccc-cc|
        00000050  63 63 63 63 63 3a 3a 63  63 63 63 63 63 63 00 00  |ccccc::ccccccc..|
        00000060  00 00 00 00 00 01 74 74  74 2d 74 74 74 74 74 2d  |......ttt-ttttt-|
        00000070  74 74 74 74 2d 74 74 74  74 74 74 74 2d 74 74 74  |tttt-ttttttt-ttt|
        00000080  2d 74 74 74 74 3a 3a 74  74 74 74 74 74 74 74 74  |-tttt::ttttttttt|
        00000090  74 00 00 00 00 00 00 00  01                       |t........|
        00000099
         */
        dataChan.write(bytesFor(
                0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,   0x2d, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
                0x2d, 0x62, 0x62, 0x62, 0x62, 0x2d, 0x62, 0x62,   0x62, 0x62, 0x62, 0x62, 0x62, 0x2d, 0x62, 0x62,
                0x62, 0x62, 0x3a, 0x3a, 0x62, 0x62, 0x62, 0x62,   0x62, 0x62, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x05, 0x63, 0x63, 0x63, 0x63, 0x63,   0x63, 0x63, 0x2d, 0x63, 0x63, 0x63, 0x63, 0x63,
                0x63, 0x63, 0x63, 0x63, 0x63, 0x2d, 0x63, 0x63,   0x63, 0x63, 0x63, 0x63, 0x63, 0x2d, 0x63, 0x63,
                0x63, 0x63, 0x63, 0x63, 0x63, 0x3a, 0x3a, 0x63,   0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x74, 0x74,   0x74, 0x2d, 0x74, 0x74, 0x74, 0x74, 0x74, 0x2d,
                0x74, 0x74, 0x74, 0x74, 0x2d, 0x74, 0x74, 0x74,   0x74, 0x74, 0x74, 0x74, 0x2d, 0x74, 0x74, 0x74,
                0x2d, 0x74, 0x74, 0x74, 0x74, 0x3a, 0x3a, 0x74,   0x74, 0x74, 0x74, 0x74, 0x74, 0x74, 0x74, 0x74,
                0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,   0x01
        ));
        dataChan.close();

        Path metaPath = dataPath.resolveSibling("meta");
        FileChannel metaChan = FileChannel.open(metaPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        /* meta:
        00000000  00 00 00 2b 00 00 00 06  00 00 00 05 62 62 62 62  |...+........bbbb|
        00000010  62 62 62 62 2d 62 62 62  62 62 62 62 2d 62 62 62  |bbbb-bbbbbbb-bbb|
        00000020  62 2d 62 62 62 62 62 62  62 2d 62 62 62 62 3a 3a  |b-bbbbbbb-bbbb::|
        00000030  62 62 62 62 62 57 62 74  74 74 2d 74 74 74 74 74  |bbbbbbbttt-ttttt|
        00000040  2d 74 74 74 74 2d 74 74  74 74 74 74 74 2d 74 74  |-tttt-ttttttt-tt|
        00000050  74 2d 74 74 74 74 3a 3a  74 74 74 74 74 74 74 74  |t-tttt::tttttttt|
        00000060  74 74 00 00 00 06 7f 7b  82 83 81 8f 7f 7f 7f 7f  |tt.....{........|
        00000070  7e 83 00 00 8f 7f                                 |~.....|
        00000076
         */
        metaChan.write(bytesFor(
                0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x06,   0x00, 0x00, 0x00, 0x05, 0x62, 0x62, 0x62, 0x62,
                0x62, 0x62, 0x62, 0x62, 0x2d, 0x62, 0x62, 0x62,   0x62, 0x62, 0x62, 0x62, 0x2d, 0x62, 0x62, 0x62,
                0x62, 0x2d, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,   0x62, 0x2d, 0x62, 0x62, 0x62, 0x62, 0x3a, 0x3a,
                0x62, 0x62, 0x62, 0x62, 0x62, 0x57, 0x62, 0x74,   0x74, 0x74, 0x2d, 0x74, 0x74, 0x74, 0x74, 0x74,
                0x2d, 0x74, 0x74, 0x74, 0x74, 0x2d, 0x74, 0x74,   0x74, 0x74, 0x74, 0x74, 0x74, 0x2d, 0x74, 0x74,
                0x74, 0x2d, 0x74, 0x74, 0x74, 0x74, 0x3a, 0x3a,   0x74, 0x74, 0x74, 0x74, 0x74, 0x74, 0x74, 0x74,
                0x74, 0x74, 0x00, 0x00, 0x00, 0x06, 0x7f, 0x7b,   0x82, 0x83, 0x81, 0x8f, 0x7f, 0x7f, 0x7f, 0x7f,
                0x7e, 0x83, 0x00, 0x00, 0x8f, 0x7f
        ));
        metaChan.close();

        Exception expected = null;
        try {
            new LookupMetadata(metaPath).readData(dataPath, new LookupKey("bbbbbbbb-bbbbbbb-bbbb-bbbbbbb-bbbb::bbbbbbb"));
        } catch (Exception e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(Throwables.getRootCause(expected).getMessage().contains("exceeds num data entries"));
    }

    private static ByteBuffer bytesFor(int ... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return ByteBuffer.wrap(bytes);
    }
}
