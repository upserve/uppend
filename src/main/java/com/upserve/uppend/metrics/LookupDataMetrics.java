package com.upserve.uppend.metrics;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class LookupDataMetrics implements InternalMetrics<LookupDataMetrics> {
    // Stats summed over all LookupData operations since the Uppend store was opened
    private final long flushedKeyCount;
    private final long flushCount;
    private final long flushTimer;

    private final long lookupMissCount;
    private final long lookupHitCount;

    private final long cacheMissCount;
    private final long cacheHitCount;

    private final long findKeyTimer;

    // LookupData level stats about the size of the lookups (consistent across reopen)
    private final double avgLookupDataSize;
    private final long maxLookupDataSize;
    private final long sumLookupDataSize;

    public static class Adders {
        public final LongAdder flushCounter = new LongAdder();
        public final LongAdder flushedKeyCounter = new LongAdder();
        public final LongAdder flushTimer = new LongAdder();

        public final LongAdder lookupHitCount = new LongAdder();
        public final LongAdder lookupMissCount = new LongAdder();
        public final LongAdder cacheHitCount = new LongAdder();
        public final LongAdder cacheMissCount = new LongAdder();
        public final LongAdder findKeyTimer = new LongAdder();
    }

    public LookupDataMetrics(Adders lookupDataMetricsAdders, LongSummaryStatistics lookupDataSizeStatistics) {
        this(
                lookupDataMetricsAdders.flushedKeyCounter.sum(),
                lookupDataMetricsAdders.flushCounter.sum(),
                lookupDataMetricsAdders.flushTimer.sum(),
                lookupDataMetricsAdders.lookupMissCount.sum(),
                lookupDataMetricsAdders.lookupHitCount.sum(),
                lookupDataMetricsAdders.cacheMissCount.sum(),
                lookupDataMetricsAdders.cacheHitCount.sum(),
                lookupDataMetricsAdders.findKeyTimer.sum(),
                lookupDataSizeStatistics.getAverage(),
                Math.max(lookupDataSizeStatistics.getMax(), 0),
                lookupDataSizeStatistics.getSum()
        );
    }

    private LookupDataMetrics(
            long flushedKeyCount,
            long flushCount,
            long flushTimer,
            long lookupMissCount,
            long lookupHitCount,
            long cacheMissCount,
            long cacheHitCount,
            long findKeyTimer,
            double avgLookupDataSize,
            long maxLookupDataSize,
            long sumLookupDataSize
    ) {
        this.flushedKeyCount = flushedKeyCount;
        this.flushCount = flushCount;
        this.flushTimer = flushTimer;
        this.lookupMissCount = lookupMissCount;
        this.lookupHitCount = lookupHitCount;
        this.cacheMissCount = cacheMissCount;
        this.cacheHitCount = cacheHitCount;
        this.findKeyTimer = findKeyTimer;
        this.avgLookupDataSize = avgLookupDataSize;
        this.maxLookupDataSize = maxLookupDataSize;
        this.sumLookupDataSize = sumLookupDataSize;
    }

    public long getFlushedKeyCount() {
        return flushedKeyCount;
    }

    public long getFlushCount() {
        return flushCount;
    }

    public long getFlushTimer() {
        return flushTimer;
    }

    public long getLookupMissCount() {
        return lookupMissCount;
    }

    public long getLookupHitCount() {
        return lookupHitCount;
    }

    public long getCacheMissCount() {
        return cacheMissCount;
    }

    public long getCacheHitCount() {
        return cacheHitCount;
    }

    public long getFindKeyTimer() {
        return findKeyTimer;
    }

    public double getAvgLookupDataSize() {
        return avgLookupDataSize;
    }

    public long getMaxLookupDataSize() {
        return maxLookupDataSize;
    }

    public long getSumLookupDataSize() {
        return sumLookupDataSize;
    }

    @Override
    public String toString() {
        return "LookupDataMetrics{" +
                "flushedKeyCount=" + flushedKeyCount +
                ", flushCount=" + flushCount +
                ", flushTimer=" + flushTimer +
                ", lookupMissCount=" + lookupMissCount +
                ", lookupHitCount=" + lookupHitCount +
                ", cacheMissCount=" + cacheMissCount +
                ", cacheHitCount=" + cacheHitCount +
                ", findKeyTimer=" + findKeyTimer +
                ", avgLookupDataSize=" + avgLookupDataSize +
                ", maxLookupDataSize=" + maxLookupDataSize +
                ", sumLookupDataSize=" + sumLookupDataSize +
                '}';
    }

    public String present(LookupDataMetrics previous) {
        LookupDataMetrics delta = this.minus(previous);

        return "LookupDataMetrics: Deltas{" +
                String.format(
                        "flush(%7.2f ms/, %6.2f keys/, %5d #), ",
                        Prefix.NANO.toMilli(delta.flushTimer) / Math.max(1, delta.flushCount),
                        (double) delta.flushedKeyCount / Math.max(1, delta.flushCount),
                        delta.flushCount
                ) +
                String.format("keyLookups(%5.3f%%new, %6d #exist, %6d #new), ",
                        (double) delta.lookupMissCount / Math.max(1, delta.lookupHitCount + delta.lookupMissCount) * 100,
                        delta.lookupHitCount, delta.lookupMissCount
                ) +
                String.format("searchCache(%5.3f%%hit, %6d #), ",
                        (double) delta.cacheHitCount / Math.max(1, delta.cacheHitCount + delta.cacheMissCount) * 100,
                        delta.cacheHitCount + delta.cacheMissCount
                ) +
                String.format(
                        " findKey(%7.2f us/, %5d #), ",
                        Prefix.NANO.toMicro(delta.findKeyTimer) / Math.max(1, delta.lookupHitCount + delta.lookupMissCount),
                        delta.lookupHitCount + delta.lookupMissCount
                ) +
                "}; Absolute{" +
                String.format(
                        "lookupKeys(%7.2f avg keys/, %5d max keys/, %12d #)",
                        avgLookupDataSize, maxLookupDataSize, sumLookupDataSize
                ) +
                "};";
    }

    public LookupDataMetrics minus(LookupDataMetrics other) {
        if (Objects.isNull(other)) throw new NullPointerException("LookupDataMetrics minus method argument is null");
        return new LookupDataMetrics(
                flushedKeyCount - other.flushedKeyCount,
                flushCount - other.flushCount,
                flushTimer - other.flushTimer,
                lookupMissCount - other.lookupMissCount,
                lookupHitCount - other.lookupHitCount,
                cacheMissCount - other.cacheMissCount,
                cacheHitCount - other.cacheHitCount,
                findKeyTimer - other.findKeyTimer,
                avgLookupDataSize - other.avgLookupDataSize,
                maxLookupDataSize - other.maxLookupDataSize,
                sumLookupDataSize - other.sumLookupDataSize
        );
    }
}
