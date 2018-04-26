package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VirtualBlobStoreTest {

    private String name = "blobs_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path blobsPath = rootPath.resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder();

    private VirtualPageFile virtualPageFile;

    public static int NUMBER_OF_STORES = 12;

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        virtualPageFile = new VirtualPageFile(blobsPath, NUMBER_OF_STORES, 1024, false);

//        blobStore.clear();
    }

//    @After
//    public void uninitialize() {
//        pageCache.flush();
//        fileCache.flush();
//    }

    @Test
    public void testSimple(){

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> testVirtualBlobStore(storeNumber));
    }



    public void testVirtualBlobStore(int virtualBlobStoreNumber) {

        VirtualBlobStore blobStore = new VirtualBlobStore(virtualBlobStoreNumber, virtualPageFile);

        long pos = blobStore.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobStore.append("bar".getBytes());
        assertEquals(7, pos);
        byte[] bytes = blobStore.read(0);
        assertEquals("foo", new String(bytes));
        bytes = blobStore.read(7);
        assertEquals("bar", new String(bytes));
    }

//    @Test
//    public void testClear(){
//        long pos = blobStore.append("foo".getBytes());
//        assertEquals(8, pos);
//        pos = blobStore.append("bar".getBytes());
//        assertEquals(15, pos);
//        blobStore.clear();
//        pageCache.flush();
//        pos = blobStore.append("baz".getBytes());
//        assertEquals(8, pos);
//    }

//    @Test
//    public void testClose() throws IOException {
//        assertEquals(8, blobStore.append("foo".getBytes()));
//        assertEquals(15, blobStore.append("foobar".getBytes()));
//        blobStore.flush();
//        pageCache.flush();
//        fileCache.flush();
//        blobStore = new BlobStore(blobsPath, pageCache);
//        assertEquals(25, blobStore.getPosition());
//        assertEquals("foo", new String(blobStore.read(8)));
//        assertEquals("foobar", new String(blobStore.read(15)));
//    }

    @Test
    public void testConcurrent() {

        IntStream.range(0, NUMBER_OF_STORES)
                .parallel()
                .forEach(storeNumber -> concurrentHelper(storeNumber));


    }

    public void concurrentHelper(int virtualBlobStoreNumber) {

        VirtualBlobStore blobStore = new VirtualBlobStore(virtualBlobStoreNumber, virtualPageFile);

        new Random(241898)
                .longs(100_000, 0, 10_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobStore.append(Longs.toByteArray(val));
                    //testData.put(pos, val);
                    System.out.println("Test val! " + val);
                    assertArrayEquals(bytes, blobStore.read(pos));
                });

    }

}
