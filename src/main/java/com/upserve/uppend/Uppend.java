package com.upserve.uppend;

public final class Uppend {
    private Uppend() {
    }

    public static FileAppendOnlyStoreBuilder fileAppendOnlyStore() {
        return new FileAppendOnlyStoreBuilder();
    }
}
