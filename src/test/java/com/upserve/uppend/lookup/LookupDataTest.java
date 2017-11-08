package com.upserve.uppend.lookup;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

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
        new LookupData(5, lookupDir.resolve("data"), lookupDir.resolve("meta"));
    }

    @Test
    public void testGetAndPut() throws Exception {
        LookupData data = new LookupData(5, lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey"); // len = 5
        assertEquals(Long.MIN_VALUE, data.get(key));
        data.put(key, 80);
        assertEquals(80, data.get(key));
    }

    @Test
    public void testPutThrowsWithBadKeySize() throws Exception {
        LookupData data = new LookupData(5, lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("myke");
        Exception expected = null;
        try {
            data.put(key, 80);
        } catch (Exception e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("unexpected key length"));
    }

    @Test
    public void testFlushAndClose() throws Exception {
        LookupData data = new LookupData(5, lookupDir.resolve("data"), lookupDir.resolve("meta"));
        final LookupKey key = new LookupKey("mykey"); // len = 5
        data.put(key, 80);
        data.flush();
        data.close();
        data = new LookupData(5, lookupDir.resolve("data"), lookupDir.resolve("meta"));
        assertEquals(80, data.get(key));
    }
}
