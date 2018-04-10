package com.upserve.uppend;

import com.google.common.primitives.Longs;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class BlobsTest {
    private Blobs blobs;

    private FileCache fileCache = new FileCache(256, 512, false);
    private PagedFileMapper pagedFileMapper = new PagedFileMapper(256*1024,  64, 256, fileCache);

    @Before
    public void initialize() {
        blobs = new Blobs(Paths.get("build/test/blobs"), pagedFileMapper);
        blobs.clear();
    }

    @After
    public void uninitialize() throws IOException {
        blobs.flush();
        pagedFileMapper.flush();
        fileCache.flush();
        SafeDeleting.removeDirectory(Paths.get("build/test/blobs"));
    }

    @Test
    public void testSimple() {
        long pos = blobs.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(7, pos);
        byte[] bytes = blobs.read(0);
        assertEquals("foo", new String(bytes));
        bytes = blobs.read(7);
        assertEquals("bar", new String(bytes));
    }

    @Test
    public void testClear(){
        long pos = blobs.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(7, pos);
        blobs.clear();
        pos = blobs.append("baz".getBytes());
        assertEquals(0, pos);
    }

    @Test
    public void testClose() throws IOException {
        assertEquals(0, blobs.append("foo".getBytes()));
        blobs.flush();
        pagedFileMapper.flush();
        fileCache.flush();
        blobs = new Blobs(Paths.get("build/test/blobs"), pagedFileMapper);
        assertEquals("foo", new String(blobs.read(0)));
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

                    System.out.println("Pos: " + pos + " val: " + val);
                    assertArrayEquals(bytes, blobs.read(pos));

                });

    }

}
