package com.upserve.uppend.cli.benchmark;

import java.util.LongSummaryStatistics;

public interface BenchmarkRunnable extends Runnable {
    LongSummaryStatistics getStats();
}
