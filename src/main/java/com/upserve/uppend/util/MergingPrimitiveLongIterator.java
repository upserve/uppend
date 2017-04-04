package com.upserve.uppend.util;

import java.util.*;

public class MergingPrimitiveLongIterator implements PrimitiveIterator.OfLong {
    private final PriorityQueue<PeekingPrimitiveLongIterator> iterators;

    public MergingPrimitiveLongIterator(PrimitiveIterator.OfLong ... iterators) {
        this.iterators = new PriorityQueue<>(Comparator.comparingLong(PeekingPrimitiveLongIterator::peek));
        for (PrimitiveIterator.OfLong iterator : iterators) {
            if (iterator != null && iterator.hasNext()) {
                this.iterators.add(new PeekingPrimitiveLongIterator(iterator));
            }
        }
    }

    @Override
    public long nextLong() {
        PeekingPrimitiveLongIterator iter = iterators.remove();
        long v = iter.next();
        if (iter.hasNext()) {
            iterators.add(iter);
        }
        return v;
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }
}
