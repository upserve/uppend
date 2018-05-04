package com.upserve.uppend.util;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class FuturesTest {
    @Test
    public void testGetAllSingle() {
        AtomicInteger counter = new AtomicInteger();
        ForkJoinTask<Integer> f1 = ForkJoinPool.commonPool().submit(counter::incrementAndGet);
        Futures.getAll(Collections.singletonList(f1));
        assertEquals(1, counter.get());
    }

    @Test
    public void testGetAll1000() {
        AtomicInteger counter = new AtomicInteger();
        Collection<Future> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            futures.add(ForkJoinPool.commonPool().submit(counter::incrementAndGet));
        }
        Futures.getAll(futures);
        assertEquals(futures.size(), counter.get());
    }
}
