package com.upserve.uppend.metrics;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class MutableBlobStoreMetrics implements InternalMetrics<MutableBlobStoreMetrics>{
    // Stats for summed over all MutableBlobStoreMetrics operations since the Uppend store was opened
    private final long bytesWritten;
    private final long writeCounter;
    private final long writeTimer;
    private final long bytesRead;
    private final long readCounter;
    private final long readTimer;

    // Partition level stats for the life of the mutableBlob store (Consistent across reopen)
    private final double avgMutableBlobStoreAllocatedPages;
    private final long maxMutableBlobStoreAllocatedPages;
    private final long sumMutableBlobStoreAllocatedPages;

    public static class Adders {
        public final LongAdder bytesWritten = new LongAdder();
        public final LongAdder writeCounter = new LongAdder();
        public final LongAdder writeTimer = new LongAdder();
        public final LongAdder bytesRead = new LongAdder();
        public final LongAdder readCounter = new LongAdder();
        public final LongAdder readTimer = new LongAdder();
    }

    public MutableBlobStoreMetrics(Adders mutableBlobStoreMetricsAdders, LongSummaryStatistics mutableStoreAllocatedPagesStatistics) {
        this(
                mutableBlobStoreMetricsAdders.bytesWritten.sum(),
                mutableBlobStoreMetricsAdders.writeCounter.sum(),
                mutableBlobStoreMetricsAdders.writeTimer.sum(),
                mutableBlobStoreMetricsAdders.bytesRead.sum(),
                mutableBlobStoreMetricsAdders.readCounter.sum(),
                mutableBlobStoreMetricsAdders.readTimer.sum(),
                mutableStoreAllocatedPagesStatistics.getAverage(),
                mutableStoreAllocatedPagesStatistics.getMax(),
                mutableStoreAllocatedPagesStatistics.getSum()
        );
    }

    private MutableBlobStoreMetrics(
            long bytesWritten,
            long writeCounter,
            long writeTimer,
            long bytesRead,
            long readCounter,
            long readTimer,
            double avgMutableBlobStoreAllocatedPages,
            long maxMutableBlobStoreAllocatedPages,
            long sumMutableBlobStoreAllocatedPages

    ) {
        this.bytesWritten = bytesWritten;
        this.writeCounter = writeCounter;
        this.writeTimer = writeTimer;
        this.bytesRead = bytesRead;
        this.readCounter = readCounter;
        this.readTimer = readTimer;
        this.avgMutableBlobStoreAllocatedPages = avgMutableBlobStoreAllocatedPages;
        this.maxMutableBlobStoreAllocatedPages = maxMutableBlobStoreAllocatedPages;
        this.sumMutableBlobStoreAllocatedPages = sumMutableBlobStoreAllocatedPages;
    }

    @Override
    public String toString() {
        return "MutableBlobStoreMetrics{" +
                "bytesWritten=" + bytesWritten +
                ", writeCounter=" + writeCounter +
                ", writeTimer=" + writeTimer +
                ", bytesRead=" + bytesRead +
                ", readCounter=" + readCounter +
                ", readTimer=" + readTimer +
                ", avgMutableBlobStoreAllocatedPages=" + avgMutableBlobStoreAllocatedPages +
                ", maxMutableBlobStoreAllocatedPages=" + maxMutableBlobStoreAllocatedPages +
                ", sumMutableBlobStoreAllocatedPages=" + sumMutableBlobStoreAllocatedPages +
                '}';
    }

    @Override
    public String present(MutableBlobStoreMetrics previous) {
        MutableBlobStoreMetrics delta = this.minus(previous);

        return "MutableBlobStoreMetrics: Deltas{" +
                String.format("pagesAllocated(%5d), ", delta.sumMutableBlobStoreAllocatedPages) +
                String.format(
                        "writes(%6.2f us/, %7.2f bytes/, %6d #), ",
                        InternalMetrics.Prefix.NANO.toMicro(delta.writeTimer) / Math.max(1, delta.writeCounter),
                        (double) delta.bytesWritten / Math.max(1, delta.writeCounter),
                        delta.writeCounter
                ) +
                String.format(
                        "reads(%6.2f us/, %7.2f b/, %6d #)",
                        InternalMetrics.Prefix.NANO.toMicro(delta.readTimer) / Math.max(1, delta.readCounter),
                        (double) delta.bytesRead / Math.max(1, delta.readCounter),
                        delta.readCounter
                ) +
                "}; Absolute{" +
                String.format(
                        "pageCount(%7.2f avg/, %5d max/, %9d #)",
                        avgMutableBlobStoreAllocatedPages, maxMutableBlobStoreAllocatedPages, sumMutableBlobStoreAllocatedPages
                ) +
                "};";
    }

    @Override
    public MutableBlobStoreMetrics minus(MutableBlobStoreMetrics other) {
        if (Objects.isNull(other)) throw new NullPointerException("MutableBlobStoreMetrics minus method argument is null");
        return new MutableBlobStoreMetrics(
                bytesWritten - other.bytesWritten,
                writeCounter - other.writeCounter,
                writeTimer - other.writeTimer,
                bytesRead - other.bytesRead,
                readCounter - other.readCounter,
                readTimer - other.readTimer,
                avgMutableBlobStoreAllocatedPages - other.avgMutableBlobStoreAllocatedPages,
                maxMutableBlobStoreAllocatedPages - other.maxMutableBlobStoreAllocatedPages,
                sumMutableBlobStoreAllocatedPages - other.sumMutableBlobStoreAllocatedPages
        );
    }
}