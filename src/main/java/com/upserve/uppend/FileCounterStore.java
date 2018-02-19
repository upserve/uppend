package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileCounterStore extends FileStore implements CounterStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final LongLookup lookup;

    FileCounterStore(Path dir, int flushDelaySeconds, boolean doLock, int longLookupHashSize, int longLookupWriteCacheSize) {
        super(dir, flushDelaySeconds, doLock);

        lookup = new LongLookup(
                dir.resolve("inc-lookup"),
                longLookupHashSize,
                longLookupWriteCacheSize
        );
    }

    @Override
    public long set(String partition, String key, long value) {
        log.trace("setting {}={} in partition '{}'", key, value, partition);
        return lookup.put(partition, key, value);
    }

    @Override
    public long increment(String partition, String key, long delta) {
        log.trace("incrementing by {} key '{}' in partition '{}'", delta, key, partition);
        return lookup.increment(partition, key, delta);
    }

    @Override
    public long get(String partition, String key) {
        log.trace("getting value for key '{}' in partition '{}'", key, partition);
        long val = lookup.get(partition, key);
        return val == -1 ? 0 : val;
    }

    @Override
    public Stream<String> keys(String partition) {
        log.trace("getting keys in partition '{}'", partition);
        return lookup.keys(partition);
    }

    @Override
    public Stream<String> partitions() {
        log.trace("getting partitions");
        return lookup.partitions();
    }

    @Override
    public void clear() {
        log.trace("clearing");
        lookup.clear();
    }

    @Override
    public AppendStoreStats cacheStats(){
        return new AppendStoreStats(0, 0, lookup.cacheSize(), lookup.cacheEntries(), lookup.writeCacheTasks(), 0, 0, 0);
    }
    @Override
    protected void flushInternal() {
        lookup.flush();
    }

    @Override
    protected void closeInternal() {
        lookup.close();
    }
}
