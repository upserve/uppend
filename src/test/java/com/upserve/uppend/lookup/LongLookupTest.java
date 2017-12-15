package com.upserve.uppend.lookup;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.assertArrayEquals;

public class LongLookupTest {

    @Test
    public void testKeysDepth1() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 1, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth2() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 257, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }

    @Test
    public void testKeysDepth3() throws Exception {
        Path path = Paths.get("build/test/lookup-metadata-test/LongLookupTest");
        SafeDeleting.removeDirectory(path);
        LongLookup longLookup = new LongLookup(path, 65537, LongLookup.DEFAULT_WRITE_CACHE_SIZE);
        longLookup.put("partition", "b", 1);
        longLookup.put("partition", "c", 1);
        longLookup.put("partition", "a", 1);
        assertArrayEquals(new String[] { "a", "b", "c" }, longLookup.keys("partition").sorted().toArray());
        longLookup.close();
    }
}
