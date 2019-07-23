package com.upserve.uppend.cli.benchmark;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.LongSummaryStatistics;
import java.util.function.Function;
import java.util.stream.LongStream;

public class BenchmarkWriter implements BenchmarkRunnable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LongStream longStream;
    private final Function<Long, Integer> longFunction;

    public LongSummaryStatistics getStats() {
        return result;
    }

    private LongSummaryStatistics result;

    BenchmarkWriter(LongStream longStream, Function<Long, Integer> longFunction) {
        this.longStream = longStream;
        this.longFunction = longFunction;
    }

    static BenchmarkWriter noop() {
        return new BenchmarkWriter(null, null);
    }

    public void run() {
        if (longFunction == null || longStream == null) {
            log.info("skipping writer");
            return;
        }
        log.info("starting writer...");
        long tic = -1 * System.currentTimeMillis();
        result = longStream.map(longFunction::apply).summaryStatistics();
        log.info(
                String.format(
                        "done writing in %5.2f seconds",
                        (tic + System.currentTimeMillis()) / 1000.0
                )
        );
    }
}
