package com.upserve.uppend;

import org.junit.*;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.lang.reflect.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHelper {

    public static class IoStreamHelper {
        private static final PrintStream stdout = System.out;
        private static final PrintStream stderr = System.err;

        private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        private final ByteArrayOutputStream errStream = new ByteArrayOutputStream();

        String getOutString() {
            System.out.flush();
            return outStream.toString();
        }

        String getErrString() {
            System.err.flush();
            return errStream.toString();
        }

        protected void assertStdErrContains(String expected) {
            String errStr = getErrString();
            assertTrue("didn't find expected '" + expected + "' in captured stderr: \n" + errStr, errStr.contains(expected));
        }

        protected void assertStdOutContains(String expected) {
            String outString = getOutString();
            assertTrue("didn't find expected '" + expected + "' in captured stdout: \n" + outString, outString.contains(expected));
        }

        protected void assertStdOut(String expected) {
            assertEquals("Captured standard output string not equal", expected, getOutString());
        }

        protected void assertStdErr(String expected) {
            assertEquals("Captured standard error string not equal", expected, getErrString());
        }

        protected void resetStreams() {
            System.out.flush();
            outStream.reset();
            System.err.flush();
            errStream.reset();
        }

        @Before
        public void setUpStreams() {
            outStream.reset();
            System.setOut(new PrintStream(outStream));
            errStream.reset();
            System.setErr(new PrintStream(errStream));
        }

        @After
        public void cleanUpStreams() {
            System.setOut(stdout);
            System.setErr(stderr);
        }
    }

    public static void resetLogger(Class clazz, String fieldName) throws Exception {
        setLogger(clazz, fieldName, LoggerFactory.getLogger(clazz));
    }

    public static void setLogger(Class clazz, String fieldName, org.slf4j.Logger log) throws Exception {
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
                .withLongLookupHashCount(16)
                .withLookupPageSize(16 * 1024)
                .withMetadataTTL(0);
    }

    public static CounterStoreBuilder getDefaultCounterStoreTestBuilder() {
        return new CounterStoreBuilder()
                .withStoreName("test")
                .withTargetBufferSize(16*1024*1024)
                .withMetadataPageSize(1024)
                .withLongLookupHashCount(16)
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
