package com.upserve.uppend;

import org.junit.*;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;


public class BlobsTest {
    final Blobs blobs = new Blobs(Paths.get("build/test/blobs"));

    @Before
    public void initialize() {
        blobs.clear();
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
}
