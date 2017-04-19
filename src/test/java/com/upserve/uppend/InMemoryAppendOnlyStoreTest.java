package com.upserve.uppend;

public class InMemoryAppendOnlyStoreTest extends AppendOnlyStoreTest {
    @Override
    protected AppendOnlyStore newStore() {
        return new InMemoryAppendOnlyStore("test");
    }
}
