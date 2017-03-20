package com.upserve.uppend;

import java.nio.file.Paths;

public class FileAppendOnlyStoreTest extends AppendOnlyStoreTest {
    @Override
    protected AppendOnlyStore newStore() {
        return new FileAppendOnlyStore(Paths.get("build/test/file-append-only-store"));
    }
}
