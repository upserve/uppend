package com.upserve.uppend.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.LongStream;

@Slf4j
public class Writer implements Runnable {
    private final LongStream longStream;
    private final Function<Long, Integer> longFunction;
    public final AtomicLong bytesWritten = new AtomicLong();
    public final AtomicLong writeCount = new AtomicLong();

    public Writer(LongStream longStream, Function<Long, Integer> longFunction){
        this.longStream = longStream;
        this.longFunction = longFunction;
    }

    public static Writer noop(){
        return new Writer(null, null);
    }

    public void run(){
        if (longFunction == null || longStream == null) {
            log.info("skipping writer");
            return;
        }
        log.info("starting writer...");
        long tic = -1*System.currentTimeMillis();
        longStream.forEach(i -> {
            bytesWritten.addAndGet(longFunction.apply(i));
            writeCount.addAndGet(1);
        });
        log.info(
                String.format(
                        "done writing %d with %8.3e bytes in %5.2f seconds",
                        writeCount.get(),
                        (double)bytesWritten.get(),
                        (tic + System.currentTimeMillis())/1000.0
                )
        );
    }
}
