package com.upserve.uppend.cli.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;
import java.util.stream.LongStream;

@Slf4j
public class BenchmarkWriter implements Runnable {
    private final LongStream longStream;
    private final Function<Long, Integer> longFunction;

    BenchmarkWriter(LongStream longStream, Function<Long, Integer> longFunction){
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
        long tic = -1*System.currentTimeMillis();
        longStream.forEach(longFunction::apply);
        log.info(
                String.format(
                        "done writing in %5.2f seconds",
                        (tic + System.currentTimeMillis())/1000.0
                )
        );
    }
}
