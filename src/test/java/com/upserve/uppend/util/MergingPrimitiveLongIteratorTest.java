package com.upserve.uppend.util;

import org.apache.logging.log4j.util.Strings;
import org.junit.Test;

import java.util.*;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

public class MergingPrimitiveLongIteratorTest {
    @Test
    public void testSimple() {
        long[] a = { 1, 3, 5, 7, 9 };
        long[] b = { 0, 2, 4, 6, 8 };
        MergingPrimitiveLongIterator iter = new MergingPrimitiveLongIterator(
                Arrays.stream(a).iterator(),
                Arrays.stream(b).iterator()
        );
        long[] merged = StreamSupport.longStream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false).toArray();
        assertArrayEquals(new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, merged);
    }
}
