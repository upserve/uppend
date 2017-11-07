package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
public class FileCounterStoreBuilder implements CounterStoreBuilder<FileCounterStore> {
    private Path dir;
    private int longLookupHashSize = LongLookup.DEFAULT_HASH_SIZE;
    private int longLookupWriteCacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;
    private int flushDelaySeconds = FileCounterStore.DEFAULT_FLUSH_DELAY_SECONDS;

    public FileCounterStoreBuilder withDir(Path dir) {
        this.dir = dir;
        return this;
    }

    public FileCounterStoreBuilder withLongLookupHashSize(int longLookupHashSize) {
        this.longLookupHashSize = longLookupHashSize;
        return this;
    }

    public FileCounterStoreBuilder withLongLookupWriteCacheSize(int longLookupWriteCacheSize) {
        this.longLookupWriteCacheSize = longLookupWriteCacheSize;
        return this;
    }

    public FileCounterStoreBuilder withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return this;
    }

    @Override
    public FileCounterStore build() {
        log.info("building FileCounterStore from builder: {}", this);
        return new FileCounterStore(dir, longLookupHashSize, longLookupWriteCacheSize, flushDelaySeconds);
    }

    @Override
    public String toString() {
        return "FileCounterStoreBuilder{" +
                "dir=" + dir +
                ", longLookupHashSize=" + longLookupHashSize +
                ", longLookupWriteCacheSize=" + longLookupWriteCacheSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                '}';
    }}
