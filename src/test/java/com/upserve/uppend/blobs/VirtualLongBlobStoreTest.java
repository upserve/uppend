package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VirtualLongBlobStoreTest {

    private String name = "long_blobs_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path blobsPath = rootPath.resolve(name);

    private VirtualPageFile virtualPageFile;

    private ExecutorService executorService;
    private static int NUMBER_OF_STORES = 13;

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        executorService = new ForkJoinPool();
    }

    public void setup(int pageSize){
        PageCache pageCache = new PageCache(pageSize, 1024, 4096, executorService, null);
        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, false, pageCache);
    }

    @After
    public void uninitialize() throws IOException {
        virtualPageFile.close();
        executorService.shutdown();
    }

    @Test
    public void testSimple(){
        setup(1200);
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
        VirtualLongBlobStore blobStore = new VirtualLongBlobStore(virtualBlobStoreNumber, virtualPageFile);

        long pos;
        final int recordSize = 13 + 4 + 8;

        // Write twice to this store
        pos = blobStore.append(virtualBlobStoreNumber * times, sampleValue("f", virtualBlobStoreNumber, times).getBytes());
        assertEquals(times * (recordSize * 2), pos);
        pos = blobStore.append(virtualBlobStoreNumber * times +1, sampleValue("b", virtualBlobStoreNumber, times).getBytes());
        assertEquals(recordSize + times * (recordSize * 2), pos);

        IntStream
                .rangeClosed(0, times)
                .forEach(timeCalled -> {
                            byte[] bytes;
                            long val;
                            bytes = blobStore.readBlob(timeCalled * (recordSize * 2));
                            assertEquals(sampleValue("f", virtualBlobStoreNumber, timeCalled), new String(bytes));
                            val = blobStore.readLong(timeCalled * (recordSize * 2));
                            assertEquals(val, virtualBlobStoreNumber * timeCalled);

                            bytes = blobStore.readBlob(recordSize + timeCalled * (recordSize * 2));
                            assertEquals(sampleValue("b", virtualBlobStoreNumber, timeCalled), new String(bytes));
                            val = blobStore.readLong(recordSize + timeCalled * (recordSize * 2));
                            assertEquals(val, virtualBlobStoreNumber * timeCalled + 1);
                        }
                );
    }

    private String sampleValue(String head, int virtualStoreNumber, int times) {
        return String.format("%s_%05d_%05d", head, virtualStoreNumber, times);
    }

    @Test
    public void testClear() throws IOException {
        setup(1200);
        testVirtualBlobStore(4, 0);
        testVirtualBlobStore(4, 1);

        virtualPageFile.clear();
        testVirtualBlobStore(4, 0);
        testVirtualBlobStore(4, 1);
    }

    @Test
    public void testClose() throws IOException {
        setup(1200);
        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 0));

        virtualPageFile.close();
        setup(1200);

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 1));

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber, 2));
    }

    @Test
    public void testPageAlignment() throws IOException {
        setup(40);

        VirtualLongBlobStore blobStore = new VirtualLongBlobStore(1, virtualPageFile);
        assertEquals(0, blobStore.append(1L, sampleValue("m", 1, 0).getBytes()));
        assertEquals(25, blobStore.append(2L, sampleValue("n", 1, 1).getBytes()));

        assertEquals(1L, blobStore.readLong(0));
        assertArrayEquals(sampleValue("m", 1, 0).getBytes(), blobStore.readBlob(0));
        assertEquals(2L, blobStore.readLong(25));
        assertArrayEquals(sampleValue("n", 1, 1).getBytes(), blobStore.readBlob(25));

        Page page0 = virtualPageFile.getExistingPage(1, 0);
        Page page1 = virtualPageFile.getExistingPage(1, 1);

        byte[] bytes = new byte[40];
        page0.get(0, bytes, 0);
        assertArrayEquals(new byte[]{0,0,0,13,0,0,0,0,0,0,0,1,109,95,48,48,48,48,49,95,48,48,48,48,48,0,0,0,13,0,0,0,0,0,0,0,2,110,95,48}, bytes);
        page1.get(0, bytes, 0);
        assertArrayEquals(new byte[]{48,48,48,49,95,48,48,48,48,49,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, bytes);

        virtualPageFile.clear();

        // If the Long would naturally fall on a page break - the page start is aligned
        blobStore = new VirtualLongBlobStore(1, virtualPageFile);
        assertEquals(0, blobStore.append(1L, sampleValue("mmmmmm", 1, 0).getBytes()));
        assertEquals(36, blobStore.append(2L, sampleValue("nnnnnn", 1, 1).getBytes()));

        assertEquals(1L, blobStore.readLong(0));
        assertArrayEquals(sampleValue("mmmmmm", 1, 0).getBytes(), blobStore.readBlob(0));
        assertEquals(2L, blobStore.readLong(36));
        assertArrayEquals(sampleValue("nnnnnn", 1, 1).getBytes(), blobStore.readBlob(36));

        bytes = new byte[40];
        page0.get(0, bytes, 0);
        assertArrayEquals(new byte[]{0,0,0,18,0,0,0,0,0,0,0,1,109,109,109,109,109,109,95,48,48,48,48,49,95,48,48,48,48,48,0,0,0,0,0,0,0,0,0,18}, bytes);
        page1.get(0, bytes, 0);
        assertArrayEquals(new byte[]{0,0,0,0,0,0,0,2,110,110,110,110,110,110,95,48,48,48,48,49,95,48,48,48,48,49,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, bytes);
    }

    @Test
    public void testConcurrent() {
        setup(1280);
        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(this::concurrentHelper);
    }

    private void concurrentHelper(int virtualBlobStoreNumber) {

        VirtualLongBlobStore blobStore = new VirtualLongBlobStore(virtualBlobStoreNumber, virtualPageFile);

        ConcurrentMap<Long, byte[]> testData = new ConcurrentHashMap<>();

        LongStream.range(0, 10_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobStore.append(val, bytes);

                    testData.put(pos, bytes);
                });

        testData.entrySet().parallelStream().forEach(entry -> {
            byte[] result = blobStore.readBlob(entry.getKey());
            assertArrayEquals(entry.getValue(), result);

            long val = blobStore.readLong(entry.getKey());
            assertEquals(Longs.fromByteArray(entry.getValue()), val);
        });
    }
}
