package com.upserve.uppend.performance;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;
import java.util.stream.*;

import static junit.framework.TestCase.assertTrue;

public class StreamTimerMethods {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int repeats = 5;

    public static void parallelTime(double time) {
        log.info("Parallel execution time   : {}ms", String.format("%8.2f", time));
    }

    public static void sequentialTime(double time) {
        log.info("Sequential execution time : {}ms", String.format("%8.2f", time));
    }

    public static double sum(Supplier<LongStream> supplier) {
        LongStream[] longStreams = new LongStream[repeats];
        long[] sums = new long[repeats];

        for (int i=0; i<repeats; i++) {
            longStreams[i] = supplier.get();
        }

        long tic = System.nanoTime();
        for (int i=0; i<repeats; i++) {
            // This is the thing we are testing - the terminal operator
            sums[i] = longStreams[i].sum();
        }
        long toc = System.nanoTime();

        for (int i=0; i<repeats; i++) {
            assertTrue("Should be large", sums[i] > 1_000_000L);
        }

        return (toc - tic)/1_000_000.0D;
    }

    public static double groupByCounting(Supplier<LongStream> supplier, boolean concurrent) {
        LongStream[] longStreams = new LongStream[repeats];

        List<Map<Long, Long>> groups = new ArrayList<>(repeats);

        for (int i=0; i<repeats; i++) {
            longStreams[i] = supplier.get();
        }

        long tic = System.nanoTime();

        if (concurrent) {
            for (LongStream longStream : longStreams) {
                groups.add(longStream.boxed().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())));
            }
        } else {
            for (LongStream longStream : longStreams) {
                groups.add(longStream.boxed().collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting())));
            }
        }

        long toc = System.nanoTime();

        for (Map<Long,Long> group: groups){
            assertTrue("Should be large", group.values().stream().mapToLong(Long::longValue).sum() >= 1_000_000L);
        }

        return (toc - tic)/1_000_000.0D;
    }

    public static double forEachAdder(Supplier<LongStream> supplier) {
        LongStream[] longStreams = new LongStream[repeats];
        long[] sums = new long[repeats];
        LongAdder[] longAdders = new LongAdder[repeats];

        for (int i=0; i<repeats; i++) {
            longStreams[i] = supplier.get();
            longAdders[i] = new LongAdder();
        }

        long tic = System.nanoTime();
        for (int i=0; i<repeats; i++) {
            // This is the thing we are testing - the terminal operator
            longStreams[i].forEach(longAdders[i]::add);
        }
        long toc = System.nanoTime();


        for (int i=0; i<repeats; i++) {
            assertTrue("Should be large", longAdders[i].longValue() > 1_000_000L);
        }

        return (toc - tic)/1_000_000.0D;
    }

    public static double flatMapSum(LongStream stream){
        long tic = System.nanoTime();
        long sum = stream.sum();
        long toc = System.nanoTime();

        assertTrue("Should be large", sum > 1_000_000L);

        return (toc - tic)/1_000_000.0D;
    }
}
