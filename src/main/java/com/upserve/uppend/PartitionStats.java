package com.upserve.uppend;

import java.util.Objects;

public class PartitionStats {

    private final int metadataPageCount;
    private final int keyPageCount;
    private final int blobPageCount;

    private final long metadataLookupMissCount;
    private final long metadataLookupHitCount;

    public PartitionStats(int metadataPageCount, int keyPageCount, int blobPageCount, long metadataLookupMissCount, long metadataLookupHitCount) {
        this.metadataPageCount = metadataPageCount;
        this.keyPageCount = keyPageCount;
        this.blobPageCount = blobPageCount;
        this.metadataLookupMissCount = metadataLookupMissCount;
        this.metadataLookupHitCount = metadataLookupHitCount;
    }

    public static PartitionStats ZERO_STATS = new PartitionStats(0,0,0,0,0);

    public int getMetadataPageCount() {
        return metadataPageCount;
    }

    public int getKeyPageCount() {
        return keyPageCount;
    }

    public int getBlobPageCount() {
        return blobPageCount;
    }

    public long getMetadataLookupHitCount() {
        return metadataLookupHitCount;
    }

    public long getMetadataLookupMissCount() {
        return metadataLookupMissCount;
    }

    @Override
    public String toString() {
        return "PartitionStats{" +
                "metadataPageCount=" + metadataPageCount +
                ", keyPageCount=" + keyPageCount +
                ", blobPageCount=" + blobPageCount +
                ", metadataLookupMissCount=" + metadataLookupMissCount +
                ", metadataLookupHitCount=" + metadataLookupHitCount +
                '}';
    }

    public PartitionStats minus(PartitionStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("PartitionStats minus method argument is null");
        return new PartitionStats(
                metadataPageCount - other.metadataPageCount,
                keyPageCount - other.keyPageCount,
                blobPageCount - other.blobPageCount,
                metadataLookupMissCount - other.metadataLookupMissCount,
                metadataLookupHitCount - other.metadataLookupHitCount
        );
    }

    public PartitionStats add(PartitionStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("PartitionStats add method argument is null");
        return new PartitionStats(
                metadataPageCount + other.metadataPageCount,
                keyPageCount + other.keyPageCount,
                blobPageCount + other.blobPageCount,
                metadataLookupMissCount + other.metadataLookupMissCount,
                metadataLookupHitCount + other.metadataLookupHitCount
        );
    }
}

