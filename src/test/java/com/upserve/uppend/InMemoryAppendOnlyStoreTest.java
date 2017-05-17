package com.upserve.uppend;

import org.junit.Test;

import java.nio.file.Paths;

public class InMemoryAppendOnlyStoreTest extends AppendOnlyStoreTest {
    @Override
    protected AppendOnlyStore newStore() {
        return new InMemoryAppendOnlyStore("test");
    }

    @Test
    public void testReservations() throws Exception{
        testReservations(Paths.get("build/test/reserved-store"), path-> new FileAppendOnlyStore(path));
    }
}
