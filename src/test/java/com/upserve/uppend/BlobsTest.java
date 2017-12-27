package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;


public class BlobsTest {
    private Blobs blobs;

    @Before
    public void initialize() {
        blobs = new Blobs(Paths.get("build/test/blobs"));
        blobs.clear();
    }

    @After
    public void uninitialize() throws IOException {
        blobs.close();
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
    public void testClose(){
        assertEquals(0, blobs.append("foo".getBytes()));
        blobs.close();
        blobs.close();
        blobs = new Blobs(Paths.get("build/test/blobs"));
        assertEquals("foo", new String(blobs.read(0)));
    }
}
