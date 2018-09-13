package com.upserve.uppend.lookup;

import java.util.Objects;

public class PartitionLookupKey {
    private final LookupKey lookupKey;
    private final String partition;

    public PartitionLookupKey(String partition, LookupKey lookupKey) {
        this.partition = partition;
        this.lookupKey = lookupKey;
    }

    public LookupKey getLookupKey() {
        return lookupKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionLookupKey that = (PartitionLookupKey) o;
        return Objects.equals(lookupKey, that.lookupKey) &&
                Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lookupKey, partition);
    }

    public int weight() {
        return lookupKey.byteLength();
    }
}
