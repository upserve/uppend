package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
public class FileIncrementOnlyStoreBuilder implements IncrementOnlyStoreBuilder<FileIncrementOnlyStore> {
    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;
    private int flushDelaySeconds = FileIncrementOnlyStore.DEFAULT_FLUSH_DELAY_SECONDS;

    public FileIncrementOnlyStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public FileIncrementOnlyStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.longLookupHashSize = longLookupHashSize;
        return this;
    }

    public FileIncrementOnlyStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    public FileIncrementOnlyStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    @Override
    public FileIncrementOnlyStore build() {
        log.info("building FileIncrementOnlyStore from builder: {}", this);
        return new FileIncrementOnlyStore(dir, longLookupHashSize, longLookupWriteCacheSize, flushDelaySeconds);
    }

    @Override
    public String toString() {
        return "FileIncrementOnlyStoreBuilder{" +
                "dir=" + dir +
                ", longLookupHashSize=" + longLookupHashSize +
                ", longLookupWriteCacheSize=" + longLookupWriteCacheSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                '}';
    }}
