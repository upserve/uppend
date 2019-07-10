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
                .withLongLookupHashSize(16)
                .withLookupPageSize(16 * 1024)
                .withMetadataTTL(0);
    }

    public static CounterStoreBuilder getDefaultCounterStoreTestBuilder() {
        return new CounterStoreBuilder()
                .withStoreName("test")
                .withTargetBufferSize(16*1024*1024)
                .withMetadataPageSize(1024)
                .withLongLookupHashSize(16)
                .withLookupPageSize(16 * 1024);
    }

    public static int compareByteArrays(byte[] o1, byte[] o2) {
        if (o1 == null) {
            return o2 == null ? 0 : -1;
        }
        if (o2 == null) {
            return 1;
        }
        for (int i = 0; i < o1.length && i < o2.length; i++) {
            int v1 = 0xff & o1[i];
            int v2 = 0xff & o2[i];
            if (v1 != v2) {
                return v1 < v2 ? -1 : 1;
            }
        }
        return Integer.compare(o1.length, o2.length);
    }

    public static byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
