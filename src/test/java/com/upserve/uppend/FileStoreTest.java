package com.upserve.uppend;

import com.upserve.uppend.blobs.VirtualPageFile;
import com.upserve.uppend.lookup.LookupCache;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.*;

public class FileStoreTest {
    private final Path path = Paths.get("build/test/tmp/file-store");
    private final Path pageFilesPath = Paths.get("build/test/tmp/file-store-page-files");

    private VirtualPageFile partitionLongKeyFile;
    private VirtualPageFile partitionMetadataBlobFile;
    private VirtualPageFile partitionBlobsFile;
    private BlockedLongs partitionBlocks;
    private LookupCache partitionLookupCache;

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(path);
        SafeDeleting.removeTempPath(pageFilesPath);
        Files.createDirectories(pageFilesPath);
        Path partitionLongKeyPath = pageFilesPath.resolve("long-key");
        Path partitionMetadataBlobPath = pageFilesPath.resolve("metadata-blob");
        Path partitionBlobPath = pageFilesPath.resolve("blob");
        Path partitionBlocksPath = pageFilesPath.resolve("blocks");
        partitionLongKeyFile = new VirtualPageFile(partitionLongKeyPath, 36, 1024, false);
        partitionMetadataBlobFile = new VirtualPageFile(partitionMetadataBlobPath, 36, 1024, false);
        partitionBlobsFile = new VirtualPageFile(partitionBlobPath, 36, 1024, false);
        partitionBlocks = new BlockedLongs(partitionBlocksPath, 20, false);
        partitionLookupCache = new LookupCache(0, 0, ForkJoinPool.commonPool(), null);
    }

    @Test
    public void testCtor() {
        new MyFileStore(path.resolve("ctor"), 10);
    }

    @Test(expected = NullPointerException.class)
    public void testCtorNullPath() {
        new MyFileStore(null, 10);
    }

    @Test
    public void testCtorNoPartitions() {
        new MyFileStore(path.resolve("ctor-no-partitions"), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorTooHighNumPartitions() {
        new MyFileStore(path.resolve("ctor--too-high-num-partitions"), FileStore.MAX_NUM_PARTITIONS + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorNegativeNumPartitions() {
        new MyFileStore(path.resolve("ctor-negative-num-partitions"), -1);
    }

    @Test(expected = UncheckedIOException.class)
    public void testCtorPathExists() throws Exception {
        Path dir = path.resolve("ctor-path-exists");
        Files.createDirectories(path);
        Files.write(dir, new byte[] { });
        new MyFileStore(dir, 10);
    }

    @Test(expected = IllegalStateException.class)
    public void testCtorSamePath() {
        new MyFileStore(path.resolve("ctor-same-path"), 10);
        new MyFileStore(path.resolve("ctor-same-path"), 10);
    }

    @Test(expected = UncheckedIOException.class)
    public void testCtorLockPathIsDir() throws IOException {
        Path dir = path.resolve("ctor-lock-path-is-dir");
        Path lockPath = dir.resolve("writeLock");
        Files.createDirectories(lockPath);
        new MyFileStore(dir, 10);
    }

    @Test
    public void testPartitionHashExamples() {
        MyFileStore v = new MyFileStore(path.resolve("partition-hash-examples"), 10);
        assertEquals("0009", v.partitionHash("0"));
        assertEquals("0005", v.partitionHash("1"));
        assertEquals("0000", v.partitionHash("2"));
        assertEquals("0000", v.partitionHash("3"));
        assertEquals("0007", v.partitionHash("4"));
        assertEquals("0008", v.partitionHash("5"));
        assertEquals("0006", v.partitionHash("6"));
        assertEquals("0005", v.partitionHash("7"));
        assertEquals("0007", v.partitionHash("8"));
        assertEquals("0006", v.partitionHash("9"));
    }

    @Test
    public void testPartitionHashExamplesWhenNotHashed() {
        MyFileStore v = new MyFileStore(path.resolve("partition-hash-examples-when-not-hashed"), 0);
        assertEquals("0", v.partitionHash("0"));
        assertEquals("1", v.partitionHash("1"));
        assertEquals("2", v.partitionHash("2"));
        assertEquals("3", v.partitionHash("3"));
        assertEquals("4", v.partitionHash("4"));
        assertEquals("5", v.partitionHash("5"));
        assertEquals("6", v.partitionHash("6"));
        assertEquals("7", v.partitionHash("7"));
        assertEquals("8", v.partitionHash("8"));
        assertEquals("9", v.partitionHash("9"));
    }

    @Test
    public void testGetIfPresent() {
        MyFileStore v = new MyFileStore(path.resolve("get-if-present"), 0);
        Optional<AppendStorePartition> optPartition = v.getIfPresent("my-partition");
        assertFalse(optPartition.isPresent());
        v.getCreatePartitionFunction().apply("my-partition");
        v.getOrCreate("my-partition");
        optPartition = v.getIfPresent("my-partition");
        assertTrue(optPartition.isPresent());
    }

    @Test
    public void testGetOrCreate() {
        MyFileStore v = new MyFileStore(path.resolve("get-or-create"), 0);
        AppendStorePartition partition = v.getOrCreate("my-partition");
        assertNotNull(partition);
        AppendStorePartition partition2 = v.getOrCreate("my-partition");
        assertSame(partition, partition2);
    }

    @Test
    public void testStreamPartitionsEmpty() {
        MyFileStore v = new MyFileStore(path.resolve("stream-partitions-empty"), 0);
        assertEquals(0, v.streamPartitions().count());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testStreamPartitionsNonEmpty() {
        MyFileStore v = new MyFileStore(path.resolve("stream-partitions-non-empty"), 0);
        v.getOrCreate("p1").append("k1", "v1".getBytes());
        v.getOrCreate("p2").append("k2", "v2".getBytes());
        v.flush();
        //flush(v);
        //v.close();
        //v = new MyFileStore(path.resolve("stream-partitions-non-empty"), 0);
        assertEquals(2, v.streamPartitions().count());
        assertArrayEquals(new String[] { "k1" }, v.streamPartitions().findFirst().get().keys().toArray());
        assertArrayEquals(new String[] { "k2" }, v.streamPartitions().skip(1).findFirst().get().keys().toArray());
    }

    @Test
    public void testStreamPartitionsBadData() throws Exception {
        Path dir = path.resolve("stream-partitions-bad-data");
        MyFileStore v = new MyFileStore(dir, 0);
        Files.write(dir.resolve("partitions"), new byte[] { });
        v.streamPartitions(); // current expected behavior is for this to return empty
    }

    private void flush(FileStore store) {
        store.flush();
        try {
            partitionLongKeyFile.flush();
            partitionMetadataBlobFile.flush();
            partitionBlobsFile.flush();
            partitionBlocks.flush();
            partitionLookupCache.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("trouble flushing", e);
        }
    }

    private class MyFileStore extends FileAppendOnlyStore {
        MyFileStore(Path dir, int numPartitions) {
            super(false, new AppendOnlyStoreBuilder()
                    .withDir(dir)
                    .withPartitionSize(numPartitions)
            );
        }
    }
}
