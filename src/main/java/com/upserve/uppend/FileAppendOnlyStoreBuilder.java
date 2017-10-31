package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
public class FileAppendOnlyStoreBuilder implements AppendOnlyStoreBuilder<FileAppendOnlyStore> {
    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;
    private int flushDelaySeconds = FileAppendOnlyStore.DEFAULT_FLUSH_DELAY_SECONDS;

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

    public FileAppendOnlyStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    @Override
    public FileAppendOnlyStore build() {
        log.info("building FileAppendOnlyStore from builder: {}", this);
        return new FileAppendOnlyStore(dir, longLookupHashSize, longLookupWriteCacheSize, flushDelaySeconds);
    }

    @Override
    public String toString() {
        return "FileAppendOnlyStoreBuilder{" +
                "dir=" + dir +
                ", longLookupHashSize=" + longLookupHashSize +
                ", longLookupWriteCacheSize=" + longLookupWriteCacheSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                '}';
    }
}
