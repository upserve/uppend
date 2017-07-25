package com.upserve.uppend.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public class Reader implements Runnable {
    private final IntStream intStream;
    private final Function<Integer, Integer> integerFunction;
    public final AtomicLong bytesRead = new AtomicLong();
    public final AtomicLong readCount = new AtomicLong();

    public Reader(IntStream intStream, Function<Integer, Integer> integerFunction){
        this.intStream = intStream;
        this.integerFunction = integerFunction;
    }

    public static Reader noop(){
        return new Reader(null, null);
    }

    public void run(){
        if (integerFunction == null || intStream == null) {
            log.info("skipping reader");
            return;
        }
        log.info("starting reader...");
        long tic = -1*System.currentTimeMillis();
        intStream.forEach(i -> {
            bytesRead.addAndGet(integerFunction.apply(i));
            readCount.addAndGet(1);
        });
        log.info(
                String.format(
                        "done reading %d byte arrays, total %8.3e bytes in %5.2f seconds",
                        readCount.get(),
                        (double) bytesRead.get(),
                        (tic + System.currentTimeMillis()) / 1000.0
                )
        );    }
}
