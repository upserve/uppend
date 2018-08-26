package com.upserve.uppend;

import java.util.Objects;

public class BlockStats {

    private final int pagesLoaded;
    private final long size;
    private final long appendCount;
    private final long allocCount;
    private final long valuesReadCount;


    public static BlockStats ZERO_STATS = new BlockStats(0,0,0,0,0);

    public BlockStats(int pagesLoaded, long size, long appendCount, long allocCount, long valuesReadCount) {
        this.pagesLoaded = pagesLoaded;
        this.size = size;
        this.appendCount = appendCount;
        this.allocCount = allocCount;
        this.valuesReadCount = valuesReadCount;
    }



    public int getPagesLoaded() {
        return pagesLoaded;
    }

    public long getSize() {
        return size;
    }

    public long getAppendCount() {
        return appendCount;
    }

    public long getAllocCount() {
        return allocCount;
    }

    public long getValuesReadCount() {
        return valuesReadCount;
    }

    @Override
    public String toString() {
        return "BlockStats{" +
                "pagesLoaded=" + pagesLoaded +
                ", size=" + size +
                ", appendCount=" + appendCount +
                ", allocCount=" + allocCount +
                '}';
    }

    public BlockStats minus(BlockStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("BlockStats minus method argument is null");
        return new BlockStats(
                pagesLoaded - other.pagesLoaded,
                size - other.size,
                appendCount - other.appendCount,
                allocCount - other.allocCount,
                valuesReadCount - other.valuesReadCount
        );
    }

    public BlockStats add(BlockStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("BlockStats add method argument is null");
        return new BlockStats(
                pagesLoaded + other.pagesLoaded,
                size + other.size,
                appendCount + other.appendCount,
                allocCount + other.allocCount,
                valuesReadCount + other.valuesReadCount
        );
    }
}
