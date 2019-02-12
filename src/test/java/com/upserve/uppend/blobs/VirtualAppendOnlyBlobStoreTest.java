package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VirtualAppendOnlyBlobStoreTest {

    private String name = "blobs_test";
    private Path rootPath = Paths.get("build/test/blobs/blob_store");
    private Path blobsPath = rootPath.resolve(name);

    private VirtualPageFile virtualPageFile;

    private static int NUMBER_OF_STORES = 13;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);

    }
    private void setup(int pageSize) {
        setup(pageSize, false);
    }

    private void setup(int pageSize, boolean readOnly) {
        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, pageSize, 16384, readOnly);
    }

    @After
    public void uninitialize() throws IOException {
        virtualPageFile.close();
    }

    @Test
    public void testGetPosition() {
        setup(4);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        assertEquals(0, store.getPosition());
        store.append("abc".getBytes());
        assertEquals(7, store.getPosition());
    }


    @Test
    public void testAppendReadEmpty() {
        setup(12);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append(new byte[]{});
        assertArrayEquals(new byte[]{}, store.read(pos));
    }

    @Test
    public void testAppendReadEmptyAtPageBoundary() {
        setup(4); // the empty value is on a new page
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append(new byte[]{});
        assertArrayEquals(new byte[]{}, store.read(pos));
    }

    @Test
    public void testAppendReadAbc() {
        setup(4);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append("abc".getBytes());
        assertArrayEquals("abc".getBytes(), store.read(pos));
    }

    @Test
    public void testReadPastPositionInValidPage() {
        setup(25);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append("abc".getBytes());
        assertArrayEquals(new byte[]{}, store.read(pos + 10));
    }

    @Test
    public void testReadPastEndOfPage() {
        setup(4);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append("abc".getBytes());
        thrown.expect(IllegalStateException.class);
        store.read(pos + 10);
    }

    @Test
    public void testReadBadPos() {
        setup(4);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append("abc".getBytes());
        thrown.expect(IllegalStateException.class);
        store.read(pos +1);
    }

    @Test
    public void testReadNegativePos() {
        setup(4);
        VirtualAppendOnlyBlobStore store = new VirtualAppendOnlyBlobStore(2, virtualPageFile);

        long pos = store.append("abc".getBytes());
        thrown.expect(IllegalArgumentException.class);
        store.read(-1);
    }

    @Test
    public void testSimple() {
        setup(25);
        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 0));

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 1));

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 2));
    }

    private void testVirtualBlobStore(int virtualBlobStoreNumber, int times) {
        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(virtualBlobStoreNumber, virtualPageFile);

        long pos;
        final int recordSize = 13 + 4;

        // Write twice to this store
        pos = blobStore.append(sampleValue("f", virtualBlobStoreNumber, times).getBytes());
        assertEquals(times * (recordSize * 2), pos);
        pos = blobStore.append(sampleValue("b", virtualBlobStoreNumber, times).getBytes());
        assertEquals(recordSize + times * (recordSize * 2), pos);

        IntStream
                .rangeClosed(0, times)
                .forEach(timeCalled -> {
                            byte[] bytes;
                            bytes = blobStore.read(timeCalled * (recordSize * 2));
                            assertEquals(sampleValue("f", virtualBlobStoreNumber, timeCalled), new String(bytes));
                            bytes = blobStore.read(recordSize + timeCalled * (recordSize * 2));
                            assertEquals(sampleValue("b", virtualBlobStoreNumber, timeCalled), new String(bytes));
                        }
                );
    }

    private String sampleValue(String head, int virtualStoreNumber, int times) {
        return String.format("%s_%05d_%05d", head, virtualStoreNumber, times);
    }

    @Test
    public void testClose() throws IOException {
        setup(25);
        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 0));

        virtualPageFile.close();
        setup(25);

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 1));

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 2));
    }

    @Test
    public void testConcurrent() {
        setup(1280);
        IntStream.range(0, NUMBER_OF_STORES)
                .forEach(this::concurrentHelper);
    }

    private void concurrentHelper(int virtualBlobStoreNumber) {
        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(virtualBlobStoreNumber, virtualPageFile);

        ConcurrentMap<Long, byte[]> testData = new ConcurrentHashMap<>();

        LongStream.range(0, 200_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobStore.append(bytes);

                    testData.put(pos, bytes);
                });

        testData.entrySet().parallelStream().forEach(entry -> {
            byte[] result = blobStore.read(entry.getKey());
            assertArrayEquals(entry.getValue(), result);
        });
    }
}
