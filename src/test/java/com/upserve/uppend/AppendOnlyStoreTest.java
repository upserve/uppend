package com.upserve.uppend;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class AppendOnlyStoreTest {
    protected abstract AppendOnlyStore newStore();

    protected AppendOnlyStore store = newStore();

    @Before
    public void initialize() {
        store = newStore();
        store.clear();
    }

    @After
    public void cleanUp(){
        try {
            store.close();
        } catch (Exception e){
            throw new AssertionError("Should not raise: {}", e);
        }
    }

    @Test
    public void testWriteCloseReadRepeat() throws Exception {
        store.append("foo", "bar".getBytes());
        store.close();
        store = newStore();
        store.append("foo", "baz".getBytes());
        store.append("qux", "xyzzy".getBytes());
        store.close();
        store = newStore();
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        store.read("foo", bytes -> results.add(new String(bytes)));
        assertArrayEquals(new String[] {"bar", "baz"}, results.stream().sorted().toArray(String[]::new));
        results.clear();
        store.read("qux", bytes -> results.add(new String(bytes)));
        assertArrayEquals(new String[] {"xyzzy"}, results.stream().sorted().toArray(String[]::new));
    }

    @Test
    public void testClear(){
        String key = "foobar";

        byte[] bytes = genBytes(12);
        store.append(key, bytes);
        store.clear();
        store.read(key, x -> {throw new AssertionError("Should be clear!");} );
    }

    @Test
    public void testReadStream() {
        store.append("stream", "bar".getBytes());
        store.append("stream", "baz".getBytes());
        assertArrayEquals(new String[] { "bar", "baz" }, store.read("stream").map(String::new).sorted().toArray(String[]::new));
    }

    @Test
    public void testReadWriteSingle() {
        tester(1, 17);
    }

    @Test
    public void testReadWriteLarge() {
        tester(1, 1028);
    }

    @Test
    public void testReadWriteMulti() {
        tester(20, 17);
    }

    @Test
    public void testReadWriteMore() {
        tester(40, 17);
    }

    @Test
    public void testReadWriteZero() {
        tester(0, 17);
    }

    @Test
    public void testReadWriteEmpy() {
        tester(1, 0);
    }

    public void tester(int number, int size){
        String key = "foobar";

        byte[] bytes;
        ArrayList<byte[]> inputBytes = new ArrayList();
        for(int i=0; i<number; i++){
            bytes = genBytes(size);
            inputBytes.add(bytes);
            store.append(key, bytes);
        }

        try {
            store.close();
        } catch (Exception e){
            throw new AssertionError("Should not raise: {}", e);
        }

        store = newStore();

        List<byte[]> outputBytes = Collections.synchronizedList(new ArrayList<>());

        store.read(key, outputBytes::add);

        assertEquals(inputBytes.size(), outputBytes.size());

        inputBytes.sort(AppendOnlyStoreTest::compareByteArrays);
        outputBytes.sort(AppendOnlyStoreTest::compareByteArrays);

        for(int i=0; i<number; i++){
            assertArrayEquals("input and output byte arrays differ at index " + i, inputBytes.get(i), outputBytes.get(i));
        }
    }

    private static int compareByteArrays(byte[] o1, byte[] o2) {
        if (o1 == null) {
            if (o2 == null) {
                return 0;
            }
            return -1;
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
        if (o1.length == o2.length) {
            return 0;
        }
        if (o1.length < o2.length) {
            return -1;
        }
        return 1;
    }

    private byte[] genBytes(int len){
        byte[] bytes = new byte[12];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private void reopenStore() throws Exception {
        store.close();
        store = newStore();
    }
}
