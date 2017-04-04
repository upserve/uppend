package com.upserve.uppend.util;

import java.util.PrimitiveIterator;

public class PeekingPrimitiveLongIterator implements PrimitiveIterator.OfLong {
    private final PrimitiveIterator.OfLong wrapped;
    private boolean hasNext;
    private long next;

    public PeekingPrimitiveLongIterator(PrimitiveIterator.OfLong wrapped) {
        this.wrapped = wrapped;
        advance();
    }

    private void advance() {
        if (hasNext = wrapped.hasNext()) {
            next = wrapped.next();
        }
    }

    public long peek() {
        return next;
    }

    @Override
    public long nextLong() {
        long v = next;
        advance();
        return v;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }
}
