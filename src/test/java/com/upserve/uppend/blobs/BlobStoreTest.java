package com.upserve.uppend.blobs;

import com.google.common.primitives.Longs;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class BlobStoreTest {
    private BlobStore blobStore;

    private final String name = "blobs_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path blobsPath = rootPath.resolve(name);
    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder();

    private final FileCache fileCache = defaults.buildFileCache(false, name);
    private final PageCache pageCache = defaults.buildBlobPageCache(fileCache, name);

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        blobStore = new BlobStore(blobsPath, pageCache);
        blobStore.clear();
    }

    @After
    public void uninitialize() {
        pageCache.flush();
        fileCache.flush();
    }

    @Test
    public void testSimple() {
        long pos = blobStore.append("foo".getBytes());
        assertEquals(8, pos);
        pos = blobStore.append("bar".getBytes());
        assertEquals(15, pos);
        byte[] bytes = blobStore.read(8);
        assertEquals("foo", new String(bytes));
        bytes = blobStore.read(15);
        assertEquals("bar", new String(bytes));
    }

    @Test
    public void testClear(){
        long pos = blobStore.append("foo".getBytes());
        assertEquals(8, pos);
        pos = blobStore.append("bar".getBytes());
        assertEquals(15, pos);
        blobStore.clear();
        pageCache.flush();
        pos = blobStore.append("baz".getBytes());
        assertEquals(8, pos);
    }

    @Test
    public void testClose() throws IOException {
        assertEquals(8, blobStore.append("foo".getBytes()));
        assertEquals(15, blobStore.append("foobar".getBytes()));
        blobStore.flush();
        pageCache.flush();
        fileCache.flush();
        blobStore = new BlobStore(blobsPath, pageCache);
        assertEquals(25, blobStore.getPosition());
        assertEquals("foo", new String(blobStore.read(8)));
        assertEquals("foobar", new String(blobStore.read(15)));
    }

    @Test
    public void testConcurrent() {

        new Random(241898)
                .longs(100_000, 0, 10_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobStore.append(Longs.toByteArray(val));
                    //testData.put(pos, val);
                    assertArrayEquals(bytes, blobStore.read(pos));
                });

    }

}
