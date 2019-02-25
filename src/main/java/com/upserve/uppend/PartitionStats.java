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

    private final long flushedKeyCount;
    private final long flushCount;

    private final long lookups;
    private final long maxLookupSize;

    public PartitionStats(int metadataPageCount, int keyPageCount, int blobPageCount, long metadataLookupMissCount, long metadataLookupHitCount, long metadataSize, long findKeyTimer, long flushedKeyCount, long flushCount, long lookups, long maxLookupSize) {
        this.metadataPageCount = metadataPageCount;
        this.keyPageCount = keyPageCount;
        this.blobPageCount = blobPageCount;
        this.metadataLookupMissCount = metadataLookupMissCount;
        this.metadataLookupHitCount = metadataLookupHitCount;
        this.metadataSize = metadataSize;
        this.findKeyTimer = findKeyTimer;
        this.flushedKeyCount = flushedKeyCount;
        this.flushCount = flushCount;
        this.lookups = lookups;
        this.maxLookupSize = maxLookupSize;
    }

    public static PartitionStats ZERO_STATS = new PartitionStats(0,0,0,0,0, 0, 0, 0 ,0, 0, 0);

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

    public long getFlushedKeyCount() { return flushedKeyCount; }

    public long getFlushCount() { return flushCount; }

    public long getLookupCount() { return lookups; }

    public long getMaxLookupSize() { return maxLookupSize; }

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
                ", flushedKeyCount=" + flushedKeyCount +
                ", flushCount=" + flushCount +
                ", lookups=" + lookups +
                ", maxLookupSize=" + maxLookupSize +
                '}';
    }

    public String present(PartitionStats previous) {
        PartitionStats deltaStats = this.minus(previous);

        long lookupCount = Math.max(1, deltaStats.metadataLookupHitCount + deltaStats.metadataLookupMissCount);
        return "PartitionStats{ Deltas: " +
                "MDPages=" + deltaStats.metadataPageCount +
                ", KeyPages=" + deltaStats.keyPageCount +
                ", BlobPages=" + deltaStats.blobPageCount +
                ", NewKeys=" + deltaStats.metadataLookupMissCount +
                ", ExistingKeys=" + deltaStats.metadataLookupHitCount +
                ", MeanLookupTime=" + deltaStats.findKeyTimer / (lookupCount * 1000)+ "us" +
                ", flushedKeys=" + deltaStats.flushedKeyCount +
                ", flushCount=" + deltaStats.flushCount +
                "; Totals:" +
                "MeanLookupSize=" + metadataSize / Math.max(lookups, 1) +
                ", MaxLookupSize=" + maxLookupSize +
                "}";
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
                findKeyTimer - other.findKeyTimer,
                flushedKeyCount - other.flushedKeyCount,
                flushCount - other.flushCount,
                lookups - other.lookups,
                maxLookupSize - other.maxLookupSize
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
                findKeyTimer + other.findKeyTimer,
                flushedKeyCount + other.flushedKeyCount,
                flushCount + other.flushCount,
                lookups + other.lookups,
                 Math.max(maxLookupSize, other.maxLookupSize)
        );
    }
}
