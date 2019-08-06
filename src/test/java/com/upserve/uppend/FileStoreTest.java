package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class FileStoreTest {
    private final Path path = Paths.get("build/test/tmp/file-store");
    private final Path pageFilesPath = Paths.get("build/test/tmp/file-store-page-files");

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(path);
        SafeDeleting.removeTempPath(pageFilesPath);
        Files.createDirectories(pageFilesPath);
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
        Files.write(dir, new byte[]{});
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
    public void testCreateCloseOpenReadOnlyClose() {
        MyFileStore store = new MyFileStore(path.resolve("create-close-open-close"), 0);
        store.getOrCreate("p1").append("k1", "v1".getBytes());
        store.close();

        store = new MyFileStore(path.resolve("create-close-open-close"), 0, true);
        byte[][] result = store.getIfPresent("p1")
                .map(partition -> partition.read("k1"))
                .map(byteStream -> byteStream.toArray(byte[][]::new))
                .orElse(new byte[][]{});
        byte[][] expected = new byte[][]{"v1".getBytes()};
        assertArrayEquals(expected, result);

        store.close();
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
        assertEquals(2, v.streamPartitions().count());
        assertArrayEquals(new String[]{"k1"}, v.streamPartitions().findFirst().get().keys().toArray());
        assertArrayEquals(new String[]{"k2"}, v.streamPartitions().skip(1).findFirst().get().keys().toArray());
    }

    @Test
    public void testStreamPartitionsBadData() throws Exception {
        Path dir = path.resolve("stream-partitions-bad-data");
        MyFileStore v = new MyFileStore(dir, 0);
        Files.write(dir.resolve("partitions"), new byte[]{});
        v.streamPartitions(); // current expected behavior is for this to return empty
    }

    @Test
    public void testCreateDirectoriesWithSymlink() throws Exception {
        Path base = Paths.get("build/test/filestore");
        SafeDeleting.removeDirectory(base);

        Path directory = base.resolve("directory");
        Path symlink = base.resolve("symlink");
        Files.createDirectories(directory);
        Files.createSymbolicLink(symlink, directory.toAbsolutePath());

        // build/test/filestore/symlink@
        new MyFileStore(symlink, 0);

        SafeDeleting.removeDirectory(base);
    }

    @Test
    public void testCreateDirectoriesWithParentSymlink() throws Exception {
        Path base = Paths.get("build/test/filestore");
        SafeDeleting.removeDirectory(base);
        Path directory = base.resolve("directory");
        Path symlink = base.resolve("symlink");
        Path symlinkChild = symlink.resolve("child");

        Files.createDirectories(directory);
        Files.createSymbolicLink(symlink, directory.toAbsolutePath());
        Files.createDirectories(directory.resolve("child"));

        // build/test/filestore/symlink@/child
        new MyFileStore(symlinkChild, 0);

        SafeDeleting.removeDirectory(directory);
        SafeDeleting.removeDirectory(base);
    }

    @Test
    public void testReaderWriter() throws InterruptedException {

        MyFileStore reader = new MyFileStore(path.resolve("reader_writer"), 4, true);
        assertEquals(0, reader.read("foo","bar").count());
        assertEquals(0, reader.scan().flatMap(Map.Entry::getValue).count());

        MyFileStore writer = new MyFileStore(path.resolve("reader_writer"), 4, false);
        writer.append("foo", "bar", "abc".getBytes());
        writer.flush();

        // The first time we load a partition it will get the latest flush keys.
        assertEquals(
                Collections.singletonList(ByteBuffer.wrap("abc".getBytes())),
                reader.read("foo","bar").map(ByteBuffer::wrap).collect(Collectors.toList())
        );
        assertEquals(
                Collections.singletonList(ByteBuffer.wrap("abc".getBytes())),
                reader.scan().flatMap(Map.Entry::getValue).map(ByteBuffer::wrap).collect(Collectors.toList())
        );

        writer.append("foo", "baz", "def".getBytes());
        writer.flush();

        // The second time we read a partition, it will not see new keys till the metadataTTL expires
        assertEquals(0, reader.read("foo","baz").count());
        assertEquals(
                Collections.singletonList(ByteBuffer.wrap("abc".getBytes())),
                reader.scan().flatMap(Map.Entry::getValue).map(ByteBuffer::wrap).collect(Collectors.toList())
        );

        // Or we force the metadata to reload (check LookupData::trim!)!
        reader.trim();

        assertEquals(
                Collections.singletonList(ByteBuffer.wrap("def".getBytes())),
                reader.read("foo","baz").map(ByteBuffer::wrap).collect(Collectors.toList())
        );
        assertEquals(
                List.of(ByteBuffer.wrap("abc".getBytes()), ByteBuffer.wrap("def".getBytes())),
                reader.scan().flatMap(Map.Entry::getValue).map(ByteBuffer::wrap).sorted().collect(Collectors.toList())
        );
    }

    private class MyFileStore extends FileAppendOnlyStore {
        MyFileStore(Path dir, int numPartitions) {
            this(dir, numPartitions, false);
        }

        MyFileStore(Path dir, int numPartitions, boolean readOnly) {
            super(readOnly, new AppendOnlyStoreBuilder()
                    .withDir(dir)
                    .withPartitionCount(numPartitions)
                    .withMetadataTTL(30)
            );
        }
    }
}
