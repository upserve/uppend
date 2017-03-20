package com.upserve.uppend;

public interface AppendOnlyStoreBuilder<T extends AppendOnlyStore> {
    T build();
}
