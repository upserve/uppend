package com.upserve.uppend.cli.benchmark;

public enum PageSize {
    small(512 * 1024),
    medium(4 * 1024 * 1024),
    large(32 * 1024 * 1024);

    private final int size;

    PageSize(int val) {
        this.size = val;
    }

    public int getSize() {
        return size;
    }
}
