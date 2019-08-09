package com.upserve.uppend.metrics;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class LongBlobStoreMetrics implements InternalMetrics<LongBlobStoreMetrics>{
    // Stats summed over all LongBlobStore operations since the Uppend store was opened
    private final long bytesAppended;
    private final long appendCounter;
    private final long appendTimer;

    private final long bytesRead;
    private final long readCounter;
    private final long readTimer;

    private final long longWrites;
    private final long longWritesTimer;

    private final long longReads;
    private final long longReadsTimer;

    // Partition level stats for the life of the longBlob store (Consistent across reopen)
    private final double avgLongBlobStoreAllocatedPages;
    private final long maxLongBlobStoreAllocatedPages;
    private final long sumLongBlobStoreAllocatedPages;

    public static class Adders {
        public final LongAdder bytesAppended = new LongAdder();
        public final LongAdder appendCounter = new LongAdder();
        public final LongAdder appendTimer = new LongAdder();

        public final LongAdder bytesRead = new LongAdder();
        public final LongAdder readCounter = new LongAdder();
        public final LongAdder readTimer = new LongAdder();

        public final LongAdder longWrites = new LongAdder();
        public final LongAdder longWriteTimer = new LongAdder();

        public final LongAdder longReads = new LongAdder();
        public final LongAdder longReadTimer = new LongAdder();
    }

    public LongBlobStoreMetrics(Adders blobStoreMetricsAdders, LongSummaryStatistics longblobStoreAllocatedPagesStatistics) {
        this(
                blobStoreMetricsAdders.bytesAppended.sum(),
                blobStoreMetricsAdders.appendCounter.sum(),
                blobStoreMetricsAdders.appendTimer.sum(),

                blobStoreMetricsAdders.bytesRead.sum(),
                blobStoreMetricsAdders.readCounter.sum(),
                blobStoreMetricsAdders.readTimer.sum(),

                blobStoreMetricsAdders.longWrites.sum(),
                blobStoreMetricsAdders.longWriteTimer.sum(),

                blobStoreMetricsAdders.longReads.sum(),
                blobStoreMetricsAdders.longReadTimer.sum(),

                longblobStoreAllocatedPagesStatistics.getAverage(),
                Math.max(longblobStoreAllocatedPagesStatistics.getMax(), 0),
                longblobStoreAllocatedPagesStatistics.getSum()
        );
    }

    private LongBlobStoreMetrics(
            long bytesAppended,
            long appendCounter,
            long appendTimer,

            long bytesRead,
            long readCounter,
            long readTimer,

            long longWrites,
            long longWritesTimer,

            long longReads,
            long longReadsTimer,

            double avgLongBlobStoreAllocatedPages,
            long maxLongBlobStoreAllocatedPages,
            long sumLongBlobStoreAllocatedPages

    ) {
        this.bytesAppended = bytesAppended;
        this.appendCounter = appendCounter;
        this.appendTimer = appendTimer;

        this.bytesRead = bytesRead;
        this.readCounter = readCounter;
        this.readTimer = readTimer;

        this.longWrites = longWrites;
        this.longWritesTimer = longWritesTimer;

        this.longReads = longReads;
        this.longReadsTimer = longReadsTimer;

        this.avgLongBlobStoreAllocatedPages = avgLongBlobStoreAllocatedPages;
        this.maxLongBlobStoreAllocatedPages = maxLongBlobStoreAllocatedPages;
        this.sumLongBlobStoreAllocatedPages = sumLongBlobStoreAllocatedPages;
    }

    @Override
    public String toString() {
        return "LongBlobStoreMetrics{" +
                "bytesAppended=" + bytesAppended +
                ", appendCounter=" + appendCounter +
                ", appendTimer=" + appendTimer +
                ", bytesRead=" + bytesRead +
                ", readCounter=" + readCounter +
                ", readTimer=" + readTimer +
                ", longWrites=" + longWrites +
                ", longWritesTimer=" + longWritesTimer +
                ", longReads=" + longReads +
                ", longReadsTimer=" + longReadsTimer +
                ", avgLongBlobStoreAllocatedPages=" + avgLongBlobStoreAllocatedPages +
                ", maxLongBlobStoreAllocatedPages=" + maxLongBlobStoreAllocatedPages +
                ", sumLongBlobStoreAllocatedPages=" + sumLongBlobStoreAllocatedPages +
                '}';
    }

    @Override
    public String present(LongBlobStoreMetrics previous) {
        LongBlobStoreMetrics delta = this.minus(previous);

        return "LongBlobStoreMetrics: Deltas{" +
                String.format("pagesAllocated(%5d), ", delta.sumLongBlobStoreAllocatedPages) +
                String.format(
                        "appends(%6.2f us/, %7.2f bytes/, %6d #), ",
                        Prefix.NANO.toMicro(delta.appendTimer) / Math.max(1, delta.appendCounter),
                        (double) delta.bytesAppended / Math.max(1, delta.appendCounter),
                        delta.appendCounter
                ) +
                String.format(
                        "reads(%6.2f us/, %7.2f b/, %6d #), ",
                        Prefix.NANO.toMicro(delta.readTimer) / Math.max(1, delta.readCounter),
                        (double) delta.bytesRead / Math.max(1, delta.readCounter),
                        delta.readCounter
                ) +

                String.format(
                        "writeLongs(%6.2f us/, %6d #), ",
                        Prefix.NANO.toMicro(delta.longWritesTimer) / Math.max(1, delta.longWrites),
                        delta.longWrites
                ) +

                String.format(
                        "readLongs(%6.2f us/, %6d #)",
                        Prefix.NANO.toMicro(delta.longReadsTimer) / Math.max(1, delta.longReads),
                        delta.longReads
                ) +

                "}; Absolute{" +
                String.format(
                        "pageCount(%7.2f avg/, %5d max/, %9d #)",
                        avgLongBlobStoreAllocatedPages, maxLongBlobStoreAllocatedPages, sumLongBlobStoreAllocatedPages
                ) +
                "};";
    }

    @Override
    public LongBlobStoreMetrics minus(LongBlobStoreMetrics other) {
        if (Objects.isNull(other)) throw new NullPointerException("LongBlobStoreMetrics minus method argument is null");
        return new LongBlobStoreMetrics(
                bytesAppended - other.bytesAppended,
                appendCounter - other.appendCounter,
                appendTimer - other.appendTimer,

                bytesRead - other.bytesRead,
                readCounter - other.readCounter,
                readTimer - other.readTimer,

                longWrites - other.longWrites,
                longWritesTimer - other.longWritesTimer,

                longReads - other.longReads,
                longReadsTimer - other.longReadsTimer,

                avgLongBlobStoreAllocatedPages - other.avgLongBlobStoreAllocatedPages,
                maxLongBlobStoreAllocatedPages - other.maxLongBlobStoreAllocatedPages,
                sumLongBlobStoreAllocatedPages - other.sumLongBlobStoreAllocatedPages
        );
    }
}

