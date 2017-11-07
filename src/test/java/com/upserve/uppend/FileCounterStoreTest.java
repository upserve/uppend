package com.upserve.uppend;

import java.nio.file.Paths;

public class FileCounterStoreTest extends CounterStoreTest {
    @Override
    protected CounterStore newStore() {
        return new FileCounterStore(Paths.get("build/test/file-append-only-store"));
    }
}
