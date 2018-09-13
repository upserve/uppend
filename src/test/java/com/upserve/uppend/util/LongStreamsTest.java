package com.upserve.uppend.util;

import org.junit.Test;

import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class LongStreamsTest {
    @Test
    public void testLazyConcatConcats() {
        LongStream stream = LongStreams.lazyConcat(
                LongStream.of(1, 2, 3),
                () -> LongStream.of(4, 5, 6)
        );
        assertArrayEquals(new long[]{1, 2, 3, 4, 5, 6}, stream.toArray());
    }

    @Test
    public void testLazyConcatIsLazy() {
        LongStream stream = LongStreams.lazyConcat(LongStream.of(1, 2, 3), () -> {
            throw new RuntimeException("expected");
        });
        PrimitiveIterator.OfLong iter = stream.iterator();
        assertTrue(iter.hasNext());
        assertEquals(1, iter.nextLong());
        assertTrue(iter.hasNext());
        assertEquals(2, iter.nextLong());
        assertTrue(iter.hasNext());
        assertEquals(3, iter.nextLong());
        Exception expected = null;
        try {
            iter.hasNext();
        } catch (Exception e) {
            expected = e;
        }
        assertNotNull(expected);
        assertTrue(expected.getMessage().contains("expected"));
    }
}
