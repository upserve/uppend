package com.upserve.uppend.util;

import org.apache.logging.log4j.util.Strings;
import org.junit.Test;

import java.util.*;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class MergingIteratorTest {
    @Test
    public void testSimple() {
        String[] a = "You're watching television.".split(" ");
        String[] b = "Suddenly you realize there's a wasp crawling on your arm.".split(" ");
        Arrays.sort(a);
        Arrays.sort(b);
        @SuppressWarnings("unchecked") MergingIterator<String> iter = new MergingIterator<>(
                Comparator.naturalOrder(),
                Arrays.stream(a).iterator(),
                Arrays.stream(b).iterator()
        );
        String merged = Strings.join(StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false).iterator(), ' ');
        assertEquals("Suddenly You're a arm. crawling on realize television. there's wasp watching you your", merged);
    }
}
