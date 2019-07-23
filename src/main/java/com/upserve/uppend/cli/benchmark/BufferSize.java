package com.upserve.uppend.cli.benchmark;

public enum BufferSize {
    small(16 * 1024 * 1024),
    medium(128 * 1024 * 1024),
    large(Integer.MAX_VALUE);

    private final int size;

    BufferSize(int val) {
        this.size = val;
    }

    public int getSize() {
        return size;
    }
}
