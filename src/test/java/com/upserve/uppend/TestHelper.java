package com.upserve.uppend;

import org.slf4j.*;

import java.lang.reflect.*;
import java.util.concurrent.*;

public class TestHelper {
    public static void resetLogger(Class clazz, String fieldName) throws Exception {
        setLogger(clazz, fieldName, LoggerFactory.getLogger(clazz));
    }

    public static void setLogger(Class clazz, String fieldName, Logger log) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, log);
    }

    public static AppendOnlyStoreBuilder getDefaultAppendStoreTestBuilder() {
        return getDefaultAppendStoreTestBuilder(ForkJoinPool.commonPool());
    }

    public static AppendOnlyStoreBuilder getDefaultAppendStoreTestBuilder(ExecutorService testService) {
        return new AppendOnlyStoreBuilder()
                .withStoreName("test")
                .withBlobPageSize(64 * 1024)
                .withBlobsPerBlock(30)
                .withTargetBufferSize(16*1024*1024)
                .withInitialLookupKeyCacheSize(64)
                .withMaximumLookupKeyCacheWeight(100 * 1024)
                .withLookupKeyCacheExecutorService(testService)
                .withLongLookupHashSize(16)
                .withLookupPageSize(16 * 1024)
                .withMetaTTL(0)
                .withCacheMetrics();

    }

    public static CounterStoreBuilder getDefaultCounterStoreTestBuilder() {
        return new CounterStoreBuilder()
                .withStoreName("test")
                .withTargetBufferSize(16*1024*1024)
                .withInitialLookupKeyCacheSize(64)
                .withMaximumLookupKeyCacheWeight(100 * 1024)
                .withMetaDataPageSize(1024)
                .withLongLookupHashSize(16)
                .withLookupPageSize(16 * 1024)
                .withCacheMetrics();

    }

}
