package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;

import java.nio.file.Path;

public class FileAppendOnlyStoreBuilder implements AppendOnlyStoreBuilder<FileAppendOnlyStore> {
    private Path dir;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;

    public void withDir(Path dir) {
        this.dir = dir;
    }

    public FileAppendOnlyStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    @Override
    public FileAppendOnlyStore build() {
        return new FileAppendOnlyStore(dir, longLookupWriteCacheSize);
    }
}
