package com.upserve.uppend.blobs;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import static junit.framework.TestCase.assertEquals;

public class NativeIOTest {

    String fname = "test_file";
    Path rootPath = Paths.get("build/test/blobs/virtual_page_file");
    Path path = rootPath.resolve(fname);

    FileChannel fc;

    @Before
    public void setUp() throws IOException {
        Files.createDirectories(rootPath);
        fc = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @After
    public void tearDown() throws IOException {
        fc.close();
        SafeDeleting.removeDirectory(rootPath);
    }

    @Test
    public void test_madvise() throws IOException {
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 57, 5329);
        NativeIO.madvise(buffer, NativeIO.Advice.Normal);
        NativeIO.madvise(buffer, NativeIO.Advice.Random);
        NativeIO.madvise(buffer, NativeIO.Advice.Sequential);
        NativeIO.madvise(buffer, NativeIO.Advice.WillNeed);
        NativeIO.madvise(buffer, NativeIO.Advice.DontNeed);
    }

    @Test
    public void test_alignedAddress() {
        long result;

        result = NativeIO.alignedAddress(2102L);
        assertEquals(0L, result);
        assertEquals(0, result % NativeIO.pageSize);

        result = NativeIO.alignedAddress(NativeIO.pageSize);
        assertEquals(NativeIO.pageSize, result);
        assertEquals(0, result % NativeIO.pageSize);

        result = NativeIO.alignedAddress(4345290809L);
        assertEquals(4345290752L, result);
        assertEquals(0, result % NativeIO.pageSize);

        result = NativeIO.alignedAddress(4517752889L);
        assertEquals(4517752832L,result);
        assertEquals(0, result % NativeIO.pageSize);
    }

    @Test
    public void test_alignedSize() {
        long result;

        result = NativeIO.alignedSize(0L, NativeIO.pageSize);
        assertEquals(NativeIO.pageSize, result);

        result = NativeIO.alignedSize(NativeIO.pageSize - 1, 2);
        assertEquals(2 * NativeIO.pageSize, result);

        result = NativeIO.alignedSize(100 * NativeIO.pageSize + 12, NativeIO.pageSize - 11);
        assertEquals(2 * NativeIO.pageSize, result);

        result = NativeIO.alignedSize(4517752889L, 5087);
        assertEquals(2 * NativeIO.pageSize, result);
    }
}
