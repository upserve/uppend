package com.upserve.uppend;

public interface CounterStoreBuilder<T extends CounterStore> {
    T build();
}
