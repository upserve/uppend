package com.upserve.uppend.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public class Writer implements Runnable {
    private final IntStream intStream;
    private final Function<Integer, Integer> integerFunction;
    public final AtomicLong bytesWritten = new AtomicLong();
    public final AtomicLong writeCount = new AtomicLong();

    public Writer(IntStream intStream, Function<Integer, Integer> integerFunction){
        this.intStream = intStream;
        this.integerFunction = integerFunction;
    }

    public static Writer noop(){
        return new Writer(null, null);
    }

    public void run(){
        if (integerFunction == null || intStream == null) {
            log.info("skipping writer");
            return;
        }
        log.info("starting writer...");
        long tic = -1*System.currentTimeMillis();
        intStream.forEach(i -> {
            bytesWritten.addAndGet(integerFunction.apply(i));
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
