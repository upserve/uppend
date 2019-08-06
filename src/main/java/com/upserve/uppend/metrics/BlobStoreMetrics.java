package com.upserve.uppend.metrics;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class BlobStoreMetrics implements InternalMetrics<BlobStoreMetrics>{

    // Stats for summed over all AppendOnlyBlobStore operations since the Uppend store was opened
    private final long bytesAppended;
    private final long appendCounter;
    private final long appendTimer;
    private final long bytesRead;
    private final long readCounter;
    private final long readTimer;

    // Partition level stats for the life of the blocked long store (Consistent on open)
    private final double avgBlobStoreAllocatedPages;
    private final long maxBlobStoreAllocatedPages;
    private final long sumBlobStoreAllocatedPages;

    public static class Adders {
        public final LongAdder bytesAppended = new LongAdder();
        public final LongAdder appendCounter = new LongAdder();
        public final LongAdder appendTimer = new LongAdder();
        public final LongAdder bytesRead = new LongAdder();
        public final LongAdder readCounter = new LongAdder();
        public final LongAdder readTimer = new LongAdder();
    }

    public BlobStoreMetrics(Adders blobStoreMetricsAdders, LongSummaryStatistics blobStoreAllocatedPagesStatistics) {
        this(
                blobStoreMetricsAdders.bytesAppended.sum(),
                blobStoreMetricsAdders.appendCounter.sum(),
                blobStoreMetricsAdders.appendTimer.sum(),
                blobStoreMetricsAdders.bytesRead.sum(),
                blobStoreMetricsAdders.readCounter.sum(),
                blobStoreMetricsAdders.readTimer.sum(),
                blobStoreAllocatedPagesStatistics.getAverage(),
                blobStoreAllocatedPagesStatistics.getMax(),
                blobStoreAllocatedPagesStatistics.getSum()
        );
    }

    private BlobStoreMetrics(
            long bytesAppended,
            long appendCounter,
            long appendTimer,
            long bytesRead,
            long readCounter,
            long readTimer,
            double avgBlobStoreAllocatedPages,
            long maxBlobStoreAllocatedPages,
            long sumBlobStoreAllocatedPages

    ) {
       this.bytesAppended = bytesAppended;
       this.appendCounter = appendCounter;
       this.appendTimer = appendTimer;
       this.bytesRead = bytesRead;
       this.readCounter = readCounter;
       this.readTimer = readTimer;
       this.avgBlobStoreAllocatedPages = avgBlobStoreAllocatedPages;
       this.maxBlobStoreAllocatedPages = maxBlobStoreAllocatedPages;
       this.sumBlobStoreAllocatedPages = sumBlobStoreAllocatedPages;
    }

    @Override
    public String toString() {
        return "BlobStoreMetrics{" +
                "bytesAppended=" + bytesAppended +
                ", appendCounter=" + appendCounter +
                ", appendTimer=" + appendTimer +
                ", bytesRead=" + bytesRead +
                ", readCounter=" + readCounter +
                ", readTimer=" + readTimer +
                ", avgBlobStoreAllocatedPages=" + avgBlobStoreAllocatedPages +
                ", maxBlobStoreAllocatedPages=" + maxBlobStoreAllocatedPages +
                ", sumBlobStoreAllocatedPages=" + sumBlobStoreAllocatedPages +
                '}';
    }

    @Override
    public String present(BlobStoreMetrics previous) {
        BlobStoreMetrics delta = this.minus(previous);

        return "BlobStoreMetrics: Deltas{" +
                String.format("pagesAllocated(%5d), ", delta.sumBlobStoreAllocatedPages) +
                String.format(
                        "appends(%6.2f us/, %7.2f bytes/, %6d #), ",
                        Prefix.NANO.toMicro(delta.appendTimer) / Math.max(1, delta.appendCounter),
                        (double) delta.bytesAppended / Math.max(1, delta.appendCounter),
                        delta.appendCounter
                ) +
                String.format(
                        "reads(%6.2f us/, %7.2f b/, %6d #)",
                        Prefix.NANO.toMicro(delta.readTimer) / Math.max(1, delta.readCounter),
                        (double) delta.bytesRead / Math.max(1, delta.readCounter),
                        delta.readCounter
                ) +
                "}; Absolute{" +
                String.format(
                        "pageCount(%7.2f avg/, %5d max/, %9d #)",
                        avgBlobStoreAllocatedPages, maxBlobStoreAllocatedPages, sumBlobStoreAllocatedPages
                ) +
                "};";
    }

    @Override
    public BlobStoreMetrics minus(BlobStoreMetrics other) {
        if (Objects.isNull(other)) throw new NullPointerException("BlobStoreMetrics minus method argument is null");
        return new BlobStoreMetrics(
                bytesAppended - other.bytesAppended,
                appendCounter - other.appendCounter,
                appendTimer - other.appendTimer,
                bytesRead - other.bytesRead,
                readCounter - other.readCounter,
                readTimer - other.readTimer,
                avgBlobStoreAllocatedPages - other.avgBlobStoreAllocatedPages,
                maxBlobStoreAllocatedPages - other.maxBlobStoreAllocatedPages,
                sumBlobStoreAllocatedPages - other.sumBlobStoreAllocatedPages
        );
    }
}
