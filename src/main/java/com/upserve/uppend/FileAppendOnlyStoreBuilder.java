package com.upserve.uppend;

import java.nio.file.Path;

public class FileAppendOnlyStoreBuilder implements AppendOnlyStoreBuilder<FileAppendOnlyStore> {
    private Path dir;

    public void withDir(Path dir) {
        this.dir = dir;
    }

    @Override
    public FileAppendOnlyStore build() {
        return new FileAppendOnlyStore(dir);
    }
}
