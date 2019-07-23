package com.upserve.uppend.performance;

import org.junit.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.*;
import java.util.stream.*;

import static com.upserve.uppend.performance.StreamTimerMethods.*;

public class ArrayLongStreamTest {

    private static final int values = 1_000_000;
    private final long[] longs = new long[values];
    private final int repeats = 5;
    private final Supplier<LongStream> parallelStream = () -> Arrays.stream(longs).parallel();
    private final Supplier<LongStream> sequentialStream = () -> Arrays.stream(longs).sequential();

    @Before
    public void loadStore() {
        Arrays.setAll(longs, (v) -> ThreadLocalRandom.current().nextLong(0, 512));
        long val = Arrays.stream(longs, 0, 100).parallel().sum();
    }

    @Test
    public void sumTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(sum(parallelStream));
            sequentialTime(sum(sequentialStream));
        }
    }

    @Test
    public void forEachAdderTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(forEachAdder(parallelStream));
            sequentialTime(forEachAdder(sequentialStream));
        }
    }

    @Test
    public void groupByCountingTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(groupByCounting(parallelStream, false));
            sequentialTime(groupByCounting(sequentialStream, false));
        }
    }

    @Test
    public void concurrentGroupByCountingTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(groupByCounting(parallelStream, true));
            sequentialTime(groupByCounting(sequentialStream, true));
        }
    }

    @Test
    public void flatMapTest(){
        for (int i=0; i<repeats; i++) {
            parallelTime(flatMapSum(LongStream.of(1,2,3,4,5,6,7,8,9,10).flatMap(val -> Arrays.stream(longs)).parallel()));
            sequentialTime(flatMapSum(LongStream.of(1,2,3,4,5,6,7,8,9,10).flatMap(val -> Arrays.stream(longs)).sequential()));
        }
    }
}
