package com.upserve.uppend;

public interface IncrementOnlyStoreBuilder<T extends IncrementOnlyStore> {
    T build();
}
