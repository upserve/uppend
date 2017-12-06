package com.upserve.uppend.lookup;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.nio.channels.FileChannel;
import java.nio.file.*;

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
    public void testGetAndPut() throws Exception {
        LookupData data = new LookupData(lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey");
        assertEquals(Long.MIN_VALUE, data.get(key));
        data.put(key, 80);
        assertEquals(80, data.get(key));
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
}
