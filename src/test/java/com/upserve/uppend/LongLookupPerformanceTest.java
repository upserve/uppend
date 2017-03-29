package com.upserve.uppend;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class LongLookupPerformanceTest {

    Path tempFile = Paths.get("build/test/lookup");

    @Before
    public void initialize() throws Exception {
        if (Files.exists(tempFile)) {
            Files.delete(tempFile);
        }

        LongLookup lookup;
        lookup = new LongLookup(tempFile);
        for(int i = 0; i < 500_000; ++i) {
            lookup.put(String.valueOf(i), i);
        }
        lookup.close();
    }

    @Test(timeout = 250)
    public void speedTest() throws Exception {
        LongLookup lookup = new LongLookup(tempFile);
        lookup.close();
    }

}
