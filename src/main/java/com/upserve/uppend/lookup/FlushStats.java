package com.upserve.uppend.lookup;

import com.upserve.uppend.BlockStats;

import java.util.Objects;

public class FlushStats {
    private final long flushedKeys;
    private final long flushedLookups;

    public FlushStats(long flushedKeys, long flushedLookups){
        this.flushedKeys = flushedKeys;
        this.flushedLookups = flushedLookups;
    }

    public static FlushStats ZERO_STATS = new FlushStats(0,0);

    public FlushStats minus(FlushStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("FlushStats minus method argument is null");
        return new FlushStats(
                flushedKeys - other.flushedKeys,
                flushedLookups - other.flushedLookups
        );
    }

    public FlushStats add(FlushStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("FlushStats add method argument is null");
        return new FlushStats(
                flushedKeys - other.flushedKeys,
                flushedLookups - other.flushedLookups
        );
    }

    @Override
    public String toString() {
        return "FlushStats{" +
                "flushedKeys=" + flushedKeys +
                ", flushedLookups=" + flushedLookups +
                '}';
    }
}
