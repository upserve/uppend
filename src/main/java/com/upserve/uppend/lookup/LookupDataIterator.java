package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.VirtualLongBlobStore;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class LookupDataIterator<T> implements Iterator<T> {

    private int keyIndex = 0;
    private final int maxKeyIndex;
    private final int numKeys;
    private final Iterator<T> writeCacheKeyIterator;
    Function<AtomicLong, T> reader;

    private final AtomicLong position = new AtomicLong();

    /**
     * Should be constructed inside a ReadLock for the LookupData to ensure a consistent snapshot

     */
    LookupDataIterator(int maxKeyIndex, int writeCacheSize, Iterator<T> writeCacheKeyIterator, Function<AtomicLong, T> reader) {
        // Get a snapshot of the keys
        this.maxKeyIndex = maxKeyIndex;
        numKeys = maxKeyIndex + writeCacheSize;
        this.writeCacheKeyIterator = writeCacheKeyIterator;
        this.reader = reader;
    }

    int getNumKeys() {
        return numKeys;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < numKeys;
    }

    @Override
    public T next() {
        keyIndex++;

        if (keyIndex < maxKeyIndex){
            return reader.apply(position); // Read but do not cache these keys - easy to add but is it helpful?
        } else {
            return writeCacheKeyIterator.next();
        }
    }
}