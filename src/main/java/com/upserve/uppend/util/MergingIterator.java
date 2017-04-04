package com.upserve.uppend.util;

import java.util.*;

public class MergingIterator<T> implements Iterator<T> {
    private final PriorityQueue<PeekingIterator<T>> iterators;

    public MergingIterator(Comparator<T> comparator, Iterator<T> ... iterators) {
        this.iterators = new PriorityQueue<>((o1, o2) -> comparator.compare(o1.peek(), o2.peek()));
        for (Iterator<T> iterator : iterators) {
            if (iterator != null && iterator.hasNext()) {
                this.iterators.add(new PeekingIterator<>(iterator));
            }
        }
    }

    @Override
    public T next() {
        PeekingIterator<T> iter = iterators.remove();
        T v = iter.next();
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
