package com.upserve.uppend.lookup;

import java.util.Iterator;
import java.util.concurrent.atomic.*;
import java.util.function.LongFunction;

public class LookupDataIterator<T> implements Iterator<T> {

    private AtomicInteger keyIndex;
    private final int[] positions;
    private final int numKeys;
    private final Iterator<T> writeCacheKeyIterator;
    LongFunction<T> reader;

    LookupDataIterator(int[] positions, int writeCacheSize, Iterator<T> writeCacheKeyIterator, LongFunction<T> reader) {
        // Get a snapshot of the keys
        this.positions = positions;
        this.writeCacheKeyIterator = writeCacheKeyIterator;
        this.reader = reader;

        numKeys = positions.length + writeCacheSize;
        keyIndex = new AtomicInteger();
    }

    int getNumKeys() {
        return numKeys;
    }

    @Override
    public boolean hasNext() {
        return keyIndex.get() < numKeys;
    }

    @Override
    public T next() {
        int index = keyIndex.getAndIncrement();

        if (index < positions.length) {
            return reader.apply(positions[index]); // Read but do not cache these keys - easy to add but is it helpful?
        } else {
            return writeCacheKeyIterator.next();
        }
    }
}