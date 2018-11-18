package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VirtualAppendOnlyBlobStoreTest {

    private String name = "blobs_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path blobsPath = rootPath.resolve(name);

    private VirtualPageFile virtualPageFile;

    private static int NUMBER_OF_STORES = 13;

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);

    }

    public void setup(int pageSize) {
        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, pageSize, 16384,false);
    }

    @After
    public void uninitialize() throws IOException {
        virtualPageFile.close();
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
                .parallel()
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
