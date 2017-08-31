package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.nio.file.*;

public class LongLookupPerformanceTest {
    private static final int INITIAL_KEYS = 500_000;

    private Path lookupDir = Paths.get("build/test/tmp/lookup");

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(lookupDir);

        LongLookup lookup;
        lookup = new LongLookup(lookupDir);
        long lastReportTime = System.currentTimeMillis();
        System.out.println("init: " + lastReportTime + ": starting");
        for(int i = 0; i < INITIAL_KEYS; ++i) {
            lookup.put("my_partition", String.valueOf(i), i);
            if (i % 100 == 0) {
                long now = System.currentTimeMillis();
                if (now - lastReportTime > 1000) {
                    lastReportTime = now;
                    System.out.println("init: " + now + ": " + i + "/" + INITIAL_KEYS);
                }

            }
        }
        System.out.println("init: " + System.currentTimeMillis() + ": done");
        lookup.close();
    }

    @Test(timeout = 100)
    public void speedTest() throws Exception {
        LongLookup lookup = new LongLookup(lookupDir);
        lookup.close();
    }

}
