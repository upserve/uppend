package com.upserve.uppend;

import java.util.Objects;

public class PartitionStats {

    private final int metadataPageCount;
    private final int keyPageCount;
    private final int blobPageCount;

    private final long metadataLookupMissCount;
    private final long metadataLookupHitCount;

    private final long metadataSize;
    private final long findKeyTimer;

    public PartitionStats(int metadataPageCount, int keyPageCount, int blobPageCount, long metadataLookupMissCount, long metadataLookupHitCount, long metadataSize, long findKeyTimer) {
        this.metadataPageCount = metadataPageCount;
        this.keyPageCount = keyPageCount;
        this.blobPageCount = blobPageCount;
        this.metadataLookupMissCount = metadataLookupMissCount;
        this.metadataLookupHitCount = metadataLookupHitCount;
        this.metadataSize = metadataSize;
        this.findKeyTimer = findKeyTimer;
    }

    public static PartitionStats ZERO_STATS = new PartitionStats(0,0,0,0,0, 0, 0);

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

    public long getMetadataSize() {
        return metadataSize;
    }

    public long getFindKeyTimer() {
        return findKeyTimer;
    }

    @Override
    public String toString() {
        return "PartitionStats{" +
                "metadataPageCount=" + metadataPageCount +
                ", keyPageCount=" + keyPageCount +
                ", blobPageCount=" + blobPageCount +
                ", metadataLookupMissCount=" + metadataLookupMissCount +
                ", metadataLookupHitCount=" + metadataLookupHitCount +
                ", metadataSize=" + metadataSize +
                ", findKeyTimer=" + findKeyTimer +
                '}';
    }

    public String present(int partitionCount, int hashCount, PartitionStats previous) {
        PartitionStats deltaStats = this.minus(previous);

        long lookupCount = Math.max(1, deltaStats.metadataLookupHitCount + deltaStats.metadataLookupMissCount);
        return "PartitionStats{" +
                "newMDPages=" + deltaStats.metadataPageCount +
                ", newKeyPages=" + deltaStats.keyPageCount +
                ", newBlobPages=" + deltaStats.blobPageCount +
                ", missCount=" + deltaStats.metadataLookupMissCount +
                ", hitCount=" + deltaStats.metadataLookupHitCount +
                ", meanSize=" + metadataSize / (partitionCount * hashCount) +
                ", meanFindTimer=" + deltaStats.findKeyTimer / (lookupCount * 1000)+
                "us}";
    }


    public PartitionStats minus(PartitionStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("PartitionStats minus method argument is null");
        return new PartitionStats(
                metadataPageCount - other.metadataPageCount,
                keyPageCount - other.keyPageCount,
                blobPageCount - other.blobPageCount,
                metadataLookupMissCount - other.metadataLookupMissCount,
                metadataLookupHitCount - other.metadataLookupHitCount,
                metadataSize - other.metadataSize,
                findKeyTimer - other.findKeyTimer
        );
    }

    public PartitionStats add(PartitionStats other) {
        if (Objects.isNull(other)) throw new NullPointerException("PartitionStats add method argument is null");
        return new PartitionStats(
                metadataPageCount + other.metadataPageCount,
                keyPageCount + other.keyPageCount,
                blobPageCount + other.blobPageCount,
                metadataLookupMissCount + other.metadataLookupMissCount,
                metadataLookupHitCount + other.metadataLookupHitCount,
                metadataSize + other.metadataSize,
                findKeyTimer + other.findKeyTimer
        );
    }
}
