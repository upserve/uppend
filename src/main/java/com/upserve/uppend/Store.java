package com.upserve.uppend;

import com.upserve.uppend.schema.StructType;

import java.util.concurrent.*;

public class Store {
    private final ConcurrentMap<String, AppendOnlyObjectStore> typedStores;

    public Store() {
        typedStores = new ConcurrentHashMap<>();
    }

    public void defineType(StructType type) {
        Class clazz = type.generateTypes().get(type.name);
        typedStores.
    }


}
