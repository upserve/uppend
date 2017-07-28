package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.test.Util;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LongLookupPerformanceTest {
    Path lookupDir = Paths.get("build/test/tmp/lookup");

    @Before
    public void initialize() throws Exception {
        Util.removeTempPath(lookupDir);

        LongLookup lookup;
        lookup = new LongLookup(lookupDir);
        for(int i = 0; i < 500_000; ++i) {
            lookup.put("my_partition", String.valueOf(i), i);
        }
        lookup.close();
    }

    @Test(timeout = 250)
    public void speedTest() throws Exception {
        LongLookup lookup = new LongLookup(lookupDir);
        lookup.close();
    }

}
