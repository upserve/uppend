package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

@Slf4j
public class FileIncrementOnlyStore implements IncrementOnlyStore, Flushable {
    /**
     * DEFAULT_FLUSH_DELAY_SECONDS is the number of seconds to wait between
     * automatically flushing writes.
     */
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = FileAppendOnlyStore.DEFAULT_FLUSH_DELAY_SECONDS;

    private final Path dir;
    private final LongLookup lookups;

    public FileIncrementOnlyStore(Path dir) {
        this(
                dir,
                LongLookup.DEFAULT_HASH_SIZE,
                LongLookup.DEFAULT_WRITE_CACHE_SIZE,
                DEFAULT_FLUSH_DELAY_SECONDS
        );
    }

    public FileIncrementOnlyStore(Path dir, int longLookupHashSize, int longLookupWriteCacheSize, int flushDelaySeconds) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        this.dir = dir;
        lookups = new LongLookup(
                dir.resolve("inc-lookups"),
                longLookupHashSize,
                longLookupWriteCacheSize
        );
        AutoFlusher.register(flushDelaySeconds, this);
    }
    @Override
    public long increment(String partition, String key) {
        return increment(partition, key, 1);
    }

    @Override
    public long increment(String partition, String key, long delta) {
        log.trace("incrementing by {} key '{}' in partition '{}'", delta, key, partition);
        //return lookups.increment(partition, key, delta);
        return -1; // TODO: fix
    }

    @Override
    public void flush() {
        log.info("flushing {}", dir);
        lookups.flush();
        log.info("flushed {}", dir);
    }

    @Override
    public long get(String partition, String key) {
        long val = lookups.get(partition, key);
        return val == -1 ? 0 : val;
    }

    @Override
    public Stream<String> keys(String partition) {
        return lookups.keys(partition);
    }

    @Override
    public Stream<String> partitions() {
        return lookups.partitions();
    }

    @Override
    public void clear() {
        log.trace("clearing");
        lookups.clear();
    }

    @Override
    public void close() throws Exception {
        log.info("closing: " + dir);
        AutoFlusher.deregister(this);
        try {
            lookups.close();
        } catch (Exception e) {
            log.error("unable to close lookups", e);
        }
    }
}
