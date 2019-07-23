package com.upserve.uppend.blobs;

import com.upserve.uppend.TestHelper;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

public class VirtualMutableBlobStoreTest {
    private String name = "mutable_blobs_test";
    private Path rootPath = Paths.get("build/test/blobs/mutable_blob_store");
    private Path mutableBlobsPath = rootPath.resolve(name);

    private VirtualPageFile virtualPageFile;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ExecutorService executorService;
    private static int NUMBER_OF_STORES = 13;

    @Before
    public void initialize() throws IOException {

        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        executorService = new ForkJoinPool();
    }

    private void setup(int pageSize) {
        virtualPageFile = new VirtualPageFile(mutableBlobsPath, NUMBER_OF_STORES, pageSize,16384,false);
    }

    @After
    public void uninitialize() throws IOException {
        virtualPageFile.close();
        executorService.shutdown();
    }

    @Test
    public void testWriteRead() {
        setup(64);
        VirtualMutableBlobStore store = new VirtualMutableBlobStore(2, virtualPageFile);

        // Write the buffer to a position thar crosses 3 pages
        byte[] bytes = TestHelper.genBytes(123);
        store.write(15, bytes);
        byte[] result = store.read(15);

        assertEquals(ByteBuffer.wrap(bytes), ByteBuffer.wrap(result));
    }

    @Test
    public void testWriteRead_badChecksum() {
        setup(64);
        VirtualMutableBlobStore store = new VirtualMutableBlobStore(2, virtualPageFile);

        byte[] bytes = TestHelper.genBytes(123);
        store.write(15, bytes);

        // overwrite part of the hash
        VirtualPageFileIO pageFileIO = new VirtualPageFileIO(2, virtualPageFile);
        pageFileIO.writeInt(19, 88); // Visible in warning message as [0, 0, 0, 88]

        thrown.expect(IllegalStateException.class);
        store.read(15);
    }

    @Test
    public void testWriteRead_badBytes() {
        setup(64);
        VirtualMutableBlobStore store = new VirtualMutableBlobStore(2, virtualPageFile);

        byte[] bytes = TestHelper.genBytes(123);
        store.write(15, bytes);

        // overwrite part of the hash
        VirtualPageFileIO pageFileIO = new VirtualPageFileIO(2, virtualPageFile);
        pageFileIO.writeInt(26, 88); // [... 0, 0, 0, 88... ] visible in warning message

        thrown.expect(IllegalStateException.class);
        store.read(15);
    }
    // TODO Add test for concurrent read write access
}
