package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;

import java.nio.file.Path;

public class FileAppendOnlyStoreBuilder implements AppendOnlyStoreBuilder<FileAppendOnlyStore> {
    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;

    public FileAppendOnlyStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public FileAppendOnlyStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.longLookupHashSize = longLookupHashSize;
        return this;
    }

    public FileAppendOnlyStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    @Override
    public FileAppendOnlyStore build() {
        return new FileAppendOnlyStore(dir, longLookupHashSize, longLookupWriteCacheSize);
    }
}
