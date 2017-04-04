package com.upserve.uppend;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class AppendOnlyStoreTest {
    protected abstract AppendOnlyStore newStore();

    protected AppendOnlyStore store = newStore();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        store.append("partition", "foo", "bar".getBytes());
        store.close();
        store = newStore();
        store.append("partition", "foo", "baz".getBytes());
        store.append("partition", "qux", "xyzzy".getBytes());
        store.close();
        store = newStore();
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        store.read("partition", "foo").map(String::new).forEach(results::add);
        assertArrayEquals(new String[] {"bar", "baz"}, results.stream().sorted().toArray(String[]::new));
        results.clear();
        store.read("partition", "qux").map(String::new).forEach(results::add);
        assertArrayEquals(new String[] {"xyzzy"}, results.stream().sorted().toArray(String[]::new));
    }

    @Test
    public void testClear(){
        String key = "foobar";

        byte[] bytes = genBytes(12);
        store.append("partition", key, bytes);
        store.clear();
        assertEquals(0, store.read("partition", key).count());
    }

    @Test
    public void testReadStream() {
        store.append("partition", "stream", "bar".getBytes());
        store.append("partition", "stream", "baz".getBytes());
        assertArrayEquals(new String[] { "bar", "baz" }, store.read("partition", "stream").map(String::new).sorted().toArray(String[]::new));
    }

    @Test
    public void testMultiPartition() {
        store.append("partition", "key", "bar".getBytes());
        store.append("partition/bar", "key", "baz".getBytes());
        store.append("partition2", "key", "bap".getBytes());
        assertArrayEquals(new String[] { "bar" }, store.read("partition", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[] { "baz" }, store.read("partition/bar", "key").map(String::new).toArray(String[]::new));
        assertArrayEquals(new String[] { "bap" }, store.read("partition2", "key").map(String::new).toArray(String[]::new));
    }

    @Test
    public void testSpecialCharacterPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("partition!", "foo", "bar".getBytes());
    }

    @Test
    public void testLeadingSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("/partition", "foo", "bar".getBytes());
    }

    @Test
    public void testTrailingSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("partition/", "foo", "bar".getBytes());
    }

    @Test
    public void testDotPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("pre.fix", "foo", "bar".getBytes());
    }

    @Test
    public void testEmptyPartPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("foo//bar", "foo", "bar".getBytes());
    }

    @Test
    public void testEmptyPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("", "foo", "bar".getBytes());
    }

    @Test
    public void testJustSlashPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("/", "foo", "bar".getBytes());
    }

    @Test
    public void testJustSlashesPartition() {
        thrown.expect(IllegalArgumentException.class);
        store.append("//", "foo", "bar".getBytes());
    }

    @Test
    public void testKeys() throws Exception {
        store.append("partition", "one", "bar".getBytes());
        store.append("partition", "two", "baz".getBytes());
        store.append("partition2", "two", "baz".getBytes());
        store.append("partition2", "three", "baz".getBytes());
        store.close();
        store = newStore();
        assertArrayEquals(new String[] { "one", "two" }, store.keys("partition").sorted().toArray(String[]::new));
    }

    @Test
    public void testPartitions() throws Exception {
        store.append("partition_one", "one", "bar".getBytes());
        store.append("partition_two", "two", "baz".getBytes());
        store.append("partition/three", "three", "bop".getBytes());
        assertArrayEquals(new String[] { "partition/three", "partition_one", "partition_two" }, store.partitions().sorted().toArray(String[]::new));
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
    public void testReadWriteEmpty() {
        tester(1, 0);
    }

    public void tester(int number, int size){
        String key = "foobar";
        String partition = "partition";

        byte[] bytes;
        ArrayList<byte[]> inputBytes = new ArrayList();
        for(int i=0; i<number; i++){
            bytes = genBytes(size);
            inputBytes.add(bytes);
            store.append(partition, key, bytes);
        }

        try {
            store.close();
        } catch (Exception e){
            throw new AssertionError("Should not raise: {}", e);
        }

        store = newStore();

        List<byte[]> outputBytes = Collections.synchronizedList(new ArrayList<>());

        store.read(partition, key).forEach(outputBytes::add);

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
}
