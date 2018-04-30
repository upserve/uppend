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

public class VirtualAppendOnlyBlobStoreTest {

    private String name = "blobs_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path blobsPath = rootPath.resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder();

    private VirtualPageFile virtualPageFile;

    ExecutorService executorService;
    private PageCache pageCache;
    private static int NUMBER_OF_STORES = 12;

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);

        executorService = new ForkJoinPool();

        pageCache = new PageCache(12800, 1024, 4096, executorService, null);
        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, false, pageCache);

        virtualPageFile.clear();
    }

    @After
    public void uninitialize() throws IOException {
        virtualPageFile.clear();
    }

    @Test
    public void testSimple(){
        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(this::testVirtualBlobStore);
    }



    private void testVirtualBlobStore(int virtualBlobStoreNumber) {

        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(virtualBlobStoreNumber, virtualPageFile);

        long pos = blobStore.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobStore.append("bar".getBytes());
        assertEquals(7, pos);
        byte[] bytes = blobStore.read(0);
        assertEquals("foo", new String(bytes));
        bytes = blobStore.read(7);
        assertEquals("bar", new String(bytes));
    }

    @Test
    public void testClear() throws IOException {
        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(4, virtualPageFile);
        testVirtualBlobStore(4);
        virtualPageFile.clear();
        testVirtualBlobStore(4);
    }

    @Test
    public void testClose() throws IOException {
        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(4, virtualPageFile);

        assertEquals(0, blobStore.append("foo".getBytes()));
        assertEquals(7, blobStore.append("foobar".getBytes()));
        virtualPageFile.close();

        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, 1024, false);
        blobStore = new VirtualAppendOnlyBlobStore(4, virtualPageFile);
        assertEquals(17, blobStore.getPosition());
        assertEquals("foo", new String(blobStore.read(0)));
        assertEquals("foobar", new String(blobStore.read(7)));
    }

    @Test
    public void testConcurrent() {

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(this::concurrentHelper);


    }

    private void concurrentHelper(int virtualBlobStoreNumber) {

        VirtualAppendOnlyBlobStore blobStore = new VirtualAppendOnlyBlobStore(virtualBlobStoreNumber, virtualPageFile);

        ConcurrentMap<Long, byte[]> testData = new ConcurrentHashMap<>();

        LongStream.range(0, 10_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobStore.append(bytes);

                    testData.put(pos, bytes);
                });


        try {
            virtualPageFile.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("flush failed", e);
        }

        testData.entrySet().parallelStream().forEach(entry -> {

            byte[] result = blobStore.read(entry.getKey());
            assertArrayEquals(entry.getValue(), result);
        });

    }

}
