package com.upserve.uppend.cli.benchmark;

public enum BenchmarkSize {
    small(1_000_000L),
    medium(10_000_000L),
    large(100_000_000L),
    huge(1_000_000_000L),
    gigantic(10_000_000_000L);

    private final long size;

    BenchmarkSize(long val) {
        this.size = val;
    }

    public long getSize() {
        return size;
    }
}
