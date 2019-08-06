package com.upserve.uppend.metrics;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class BlockedLongMetrics implements InternalMetrics<BlockedLongMetrics> {

    // Stats summed over all BlockedLongs stores since the Uppend store was opened
    private final long blockAllocationCounter;
    private final long appendCounter;
    private final long appendTimer;
    private final long readCounter;
    private final long longsReadCounter;
    private final long readTimer;
    private final long readLastCounter;
    private final long readLastTimer;

    // Partition level stats for the life of the blocked long store (Consistent on open)
    private final double avgBlocksAllocated;
    private final long maxBlocksAllocated;
    private final long sumBlocksAllocated;
    // For read only views, AppendCounter numbers are approximate, less than actual
    private final double avgAppendCounter;
    private final long maxAppendCounter;
    private final long sumAppendCounter;

    public static class Adders {
        public final LongAdder blockAllocationCounter = new LongAdder();
        public final LongAdder appendCounter = new LongAdder();
        public final LongAdder appendTimer = new LongAdder();
        public final LongAdder readCounter = new LongAdder();
        public final LongAdder longsReadCounter = new LongAdder();
        public final LongAdder readTimer = new LongAdder();
        public final LongAdder readLastCounter = new LongAdder();
        public final LongAdder readLastTimer = new LongAdder();
    }

    public BlockedLongMetrics(Adders blockedLongMetricsAdders, LongSummaryStatistics blockedLongAllocatedBlocksStatistics,
                              LongSummaryStatistics blockedLongAppendCountStatistics) {
        this(
                blockedLongMetricsAdders.blockAllocationCounter.sum(),
                blockedLongMetricsAdders.appendCounter.sum(),
                blockedLongMetricsAdders.appendTimer.sum(),
                blockedLongMetricsAdders.readCounter.sum(),
                blockedLongMetricsAdders.longsReadCounter.sum(),
                blockedLongMetricsAdders.readTimer.sum(),
                blockedLongMetricsAdders.readLastCounter.sum(),
                blockedLongMetricsAdders.readLastTimer.sum(),

                blockedLongAllocatedBlocksStatistics.getAverage(),
                blockedLongAllocatedBlocksStatistics.getMax(),
                blockedLongAllocatedBlocksStatistics.getSum(),

                blockedLongAppendCountStatistics.getAverage(),
                blockedLongAppendCountStatistics.getMax(),
                blockedLongAppendCountStatistics.getSum()
        );
    }

    private BlockedLongMetrics(
            long blockAllocationCounter,
            long appendCounter,
            long appendTimer,
            long readCounter,
            long longsReadCounter,
            long readTimer,
            long readLastCounter,
            long readLastTimer,
            double avgBlocksAllocated,
            long maxBlocksAllocated,
            long sumBlocksAllocated,
            double avgAppendCounter,
            long maxAppendCounter,
            long sumAppendCounter
    ) {
        this.blockAllocationCounter = blockAllocationCounter;
        this.appendCounter = appendCounter;
        this.appendTimer = appendTimer;
        this.readCounter = readCounter;
        this.longsReadCounter = longsReadCounter;
        this.readTimer = readTimer;
        this.readLastCounter = readLastCounter;
        this.readLastTimer = readLastTimer;
        this.avgBlocksAllocated = avgBlocksAllocated;
        this.maxBlocksAllocated = maxBlocksAllocated;
        this.sumBlocksAllocated = sumBlocksAllocated;
        this.avgAppendCounter = avgAppendCounter;
        this.maxAppendCounter = maxAppendCounter;
        this.sumAppendCounter = sumAppendCounter;
    }

    @Override
    public String toString() {
        return "BlockedLongMetrics{" +
                "blockAllocationCounter=" + blockAllocationCounter +
                ", appendCounter=" + appendCounter +
                ", appendTimer=" + appendTimer +
                ", readCounter=" + readCounter +
                ", longsReadCounter=" + longsReadCounter +
                ", readTimer=" + readTimer +
                ", readLastCounter=" + readLastCounter +
                ", readLastTimer=" + readLastTimer +
                ", avgBlocksAllocated=" + avgBlocksAllocated +
                ", maxBlocksAllocated=" + maxBlocksAllocated +
                ", sumBlocksAllocated=" + sumBlocksAllocated +
                ", avgAppendCounter=" + avgAppendCounter +
                ", maxAppendCounter=" + maxAppendCounter +
                ", sumAppendCounter=" + sumAppendCounter +
                '}';
    }

    public String present(BlockedLongMetrics previous) {
        BlockedLongMetrics delta = this.minus(previous);

        return "BlockedLongMetrics: Deltas{" +
                String.format("blocks(%5d #), ", delta.blockAllocationCounter) +
                String.format("appends(%7.2f us/, %6d #), ", Prefix.NANO.toMicro(delta.appendTimer) / Math.max(1, delta.appendCounter), delta.appendCounter) +
                String.format(
                        "reads(%7.2f us/, %7.2f vals/, %6d #), ",
                        Prefix.NANO.toMicro(delta.readTimer) / Math.max(1, delta.readCounter),
                        (double) delta.longsReadCounter / Math.max(1, delta.readCounter),
                        delta.readCounter
                ) +
                String.format("readLast(%7.2f us/, %6d #)", Prefix.NANO.toMicro(delta.readLastTimer) / Math.max(1, delta.readLastCounter), delta.readLastCounter) +
                "}; Absolute{" +
                String.format("blocks(%8.2f avg, %6d max, %8d #), ", avgBlocksAllocated, maxBlocksAllocated, sumBlocksAllocated) +
                String.format("appends(%10.2f avg, %10d max, %14d #), ", avgAppendCounter, maxAppendCounter, sumAppendCounter) +
                String.format("valsPerBlock(%8.2f avg)", (double) sumAppendCounter / Math.max(1, sumBlocksAllocated)) +
                "};";
    }

    public BlockedLongMetrics minus(BlockedLongMetrics other) {
        if (Objects.isNull(other)) throw new NullPointerException("BlockedLongMetrics minus method argument is null");
        return new BlockedLongMetrics(
                blockAllocationCounter - other.blockAllocationCounter,
                appendCounter - other.appendCounter,
                appendTimer - other.appendTimer,
                readCounter - other.readCounter,
                longsReadCounter - other.longsReadCounter,
                readTimer - other.readTimer,
                readLastCounter - other.readLastCounter,
                readLastTimer - other.readLastTimer,
                avgBlocksAllocated - other.avgBlocksAllocated,
                maxBlocksAllocated - other.maxBlocksAllocated,
                sumBlocksAllocated - other.sumBlocksAllocated,
                avgAppendCounter - other.avgAppendCounter,
                maxAppendCounter - other.maxAppendCounter,
                sumAppendCounter - other.sumAppendCounter
        );
    }
}
