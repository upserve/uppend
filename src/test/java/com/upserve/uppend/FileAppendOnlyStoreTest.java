package com.upserve.uppend;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static org.junit.Assert.fail;

public class FileAppendOnlyStoreTest extends AppendOnlyStoreTest {
    @Override
    protected AppendOnlyStore newStore() {
        return new FileAppendOnlyStore(Paths.get("build/test/file-append-only-store"));
    }

    @Test
    public void testReservations() throws Exception{
        testReservations(Paths.get("build/test/reserved-store"), path-> new FileAppendOnlyStore(path));
    }
}
