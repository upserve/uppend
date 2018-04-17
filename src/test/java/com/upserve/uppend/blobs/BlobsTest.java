package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.primitives.Longs;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class BlobsTest {
    private Blobs blobs;

    Path rootPath = Paths.get("build/test/blobs");
    Path blobsPath = rootPath.resolve("blobs_test");

    private FileCache fileCache = new FileCache(256, 512, false);
    private PagedFileMapper pagedFileMapper = new PagedFileMapper(256*1024,  64, 256, fileCache);

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        blobs = new Blobs(blobsPath, pagedFileMapper);
        blobs.clear();
    }

    @After
    public void uninitialize() {
        pagedFileMapper.flush();
        fileCache.flush();
    }

    @Test
    public void testSimple() {
        long pos = blobs.append("foo".getBytes());
        assertEquals(8, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(15, pos);
        byte[] bytes = blobs.read(8);
        assertEquals("foo", new String(bytes));
        bytes = blobs.read(15);
        assertEquals("bar", new String(bytes));
    }

    @Test
    public void testClear(){
        long pos = blobs.append("foo".getBytes());
        assertEquals(8, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(15, pos);
        blobs.clear();
        pagedFileMapper.flush();
        pos = blobs.append("baz".getBytes());
        assertEquals(8, pos);
    }

    @Test
    public void testClose() throws IOException {
        assertEquals(8, blobs.append("foo".getBytes()));
        assertEquals(15, blobs.append("foobar".getBytes()));
        blobs.flush();
        pagedFileMapper.flush();
        fileCache.flush();
        blobs = new Blobs(blobsPath, pagedFileMapper);
        assertEquals(25, blobs.getPosition());
        assertEquals("foo", new String(blobs.read(8)));
        assertEquals("foobar", new String(blobs.read(15)));
    }

    @Test
    public void testConcurrent() {

        new Random(241898)
                .longs(100_000, 0, 10_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = Longs.toByteArray(val);
                    long pos = blobs.append(Longs.toByteArray(val));
                    //testData.put(pos, val);
                    assertArrayEquals(bytes, blobs.read(pos));
                });

    }

}
