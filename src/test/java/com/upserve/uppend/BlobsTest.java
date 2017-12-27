package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.*;
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

    @Test(expected = UncheckedIOException.class)
    public void testCloseException() throws Exception {
        resetFinal(blobs, "blobs", new FileChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                return 0;
            }

            @Override
            public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                return 0;
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return 0;
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                return 0;
            }

            @Override
            public long position() throws IOException {
                return 0;
            }

            @Override
            public FileChannel position(long newPosition) throws IOException {
                return null;
            }

            @Override
            public long size() throws IOException {
                return 0;
            }

            @Override
            public FileChannel truncate(long size) throws IOException {
                return null;
            }

            @Override
            public void force(boolean metaData) throws IOException {

            }

            @Override
            public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
                return 0;
            }

            @Override
            public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
                return 0;
            }

            @Override
            public int read(ByteBuffer dst, long position) throws IOException {
                return 0;
            }

            @Override
            public int write(ByteBuffer src, long position) throws IOException {
                return 0;
            }

            @Override
            public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                return null;
            }

            @Override
            public FileLock lock(long position, long size, boolean shared) throws IOException {
                return null;
            }

            @Override
            public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                return null;
            }

            @Override
            protected void implCloseChannel() throws IOException {
                throw new IOException("expected");
            }
        });
        blobs.close();
    }

    private static void resetFinal(Object inst, String fieldName, Object val) throws Exception {
        Field field = inst.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(inst, val);
    }
}
