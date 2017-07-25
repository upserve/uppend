package com.upserve.uppend;

import java.nio.file.Path;

public class FileAppendOnlyStoreBuilder implements AppendOnlyStoreBuilder<FileAppendOnlyStore> {
    private Path dir;

    public FileAppendOnlyStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    @Override
    public FileAppendOnlyStore build() {
        return new FileAppendOnlyStore(dir);
    }
}
