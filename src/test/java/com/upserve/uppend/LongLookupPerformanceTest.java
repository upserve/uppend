package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.nio.file.*;

public class LongLookupPerformanceTest {
    private Path lookupDir = Paths.get("build/test/tmp/lookup");

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(lookupDir);

        LongLookup lookup;
        lookup = new LongLookup(lookupDir);
        for(int i = 0; i < 500_000; ++i) {
            lookup.put("my_partition", String.valueOf(i), i);
        }
        lookup.close();
    }

    @Test(timeout = 100)
    public void speedTest() throws Exception {
        LongLookup lookup = new LongLookup(lookupDir);
        lookup.close();
    }

}
