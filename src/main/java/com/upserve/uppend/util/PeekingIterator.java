package com.upserve.uppend.util;

import java.util.Iterator;

public class PeekingIterator<T> implements Iterator<T> {
    private final Iterator<T> wrapped;
    private boolean hasNext;
    private T next;

    public PeekingIterator(Iterator<T> wrapped) {
        this.wrapped = wrapped;
        advance();
    }

    private void advance() {
        if (hasNext = wrapped.hasNext()) {
            next = wrapped.next();
        }
    }

    public T peek() {
        return next;
    }

    @Override
    public T next() {
        T v = next;
        advance();
        return v;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }
}