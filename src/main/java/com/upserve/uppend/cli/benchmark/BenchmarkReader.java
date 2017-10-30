package com.upserve.uppend.cli.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.LongStream;

@Slf4j
public class BenchmarkReader implements Runnable {
    private final LongStream longStream;
    private final Function<Long, Integer> longFunction;
    final AtomicLong bytesRead = new AtomicLong();
    final AtomicLong readCount = new AtomicLong();

    BenchmarkReader(LongStream longStream, Function<Long, Integer> longFunction) {
        this.longStream = longStream;
        this.longFunction = longFunction;
    }

    static BenchmarkReader noop() {
        return new BenchmarkReader(null, null);
    }

    public void run() {
        if (longFunction == null || longStream == null) {
            log.info("skipping reader");
            return;
        }
        log.info("starting reader...");
        long tic = -1*System.currentTimeMillis();
        longStream.forEach(i -> {
            bytesRead.addAndGet(longFunction.apply(i));
            readCount.addAndGet(1);
        });
        log.info(
                String.format(
                        "done reading %d byte arrays, total %8.3e bytes in %5.2f seconds",
                        readCount.get(),
                        (double) bytesRead.get(),
                        (tic + System.currentTimeMillis()) / 1000.0
                )
        );
    }
}
