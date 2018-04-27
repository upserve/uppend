package com.upserve.uppend.blobs;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Random;

import static java.lang.Integer.min;
import static java.lang.Math.max;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MappedPageTest {
    Path rootPath = Paths.get("build/test/blobs/filepage");
    Path filePath = rootPath.resolve("testfile");
    Path readOnlyFilePath = rootPath.resolve("readOnlyTestfile");

    MappedPage rwPage;
    MappedPage roPage;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final int pageSize = 1024;

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        try(FileChannel file = FileChannel.open(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)){
            rwPage =  new MappedPage(file.map(FileChannel.MapMode.READ_WRITE, 0, pageSize));
            roPage =  new MappedPage(file.map(FileChannel.MapMode.READ_ONLY, 0, pageSize));
        }

    }

    @Test
    public void testReadOnlyPastFileSize() throws IOException {
        Files.createFile(readOnlyFilePath);
        final MappedPage page;
        thrown.expect(IOException.class);
        thrown.expectMessage("Channel not open for writing - cannot extend file to required size");
        try(FileChannel file = FileChannel.open(readOnlyFilePath, StandardOpenOption.READ)){
            page =  new MappedPage(file.map(FileChannel.MapMode.READ_ONLY, 0, pageSize));
        }
    }

    @Test
    public void testZeroOffsetZeroPositionLessThanPageSize() {
        putGetHelper(281, 0, 0);
    }

    @Test
    public void testWithOffsetZeroPositionLessThanPageSize() {
        putGetHelper(281, 39, 0);
    }

    @Test
    public void testWithOffsetWithPositionLessThanPageSize() {
        putGetHelper(281, 39, 592);
    }

    @Test
    public void testWithOffsetWithPositionGreaterThanPageSize() {
        putGetHelper(843, 39, 592);
    }

    @Test
    public void testZeroOffsetZeroPositionGreaterThanPageSize() {
        putGetHelper(1751, 0, 0);
    }

    @Test
    public void testWithOffsetZeroPositionGreaterThanPageSize() {
        putGetHelper(1751, 16, 0);
    }

    @Test
    public void testMultipleWrites(){
        putGetHelper(42, 13, 84);
        putGetHelper(95, 1, 512);

        // overwrite!
        putGetHelper(73, 5, 81);
    }

    @Test
    public void testMultipleMaps(){
        putGetHelper(rwPage, roPage, 42, 13, 84);
        putGetHelper(rwPage, roPage, 95, 1, 512);

        // overwrite!
        putGetHelper(rwPage, roPage, 73, 5, 81);
    }

    /**
     *                        | region of comparison  |
     *              ______________________________________________
     *             |  offset  |   expectedSize        |  unused   |  Buffer
     *             *––––––––––––––––––––––––––––––––––––––––––––––*
     *   _____________________________________________
     *  |   pagePosition      |   expectedSize        |  Page
     *  *–––––––––––––––––––––––––––––––––––––––––––––*
     *
     * @param bufferSize The size of the byte[] buffer to test with
     * @param bufferOffset The offset from which to test in the buffer
     * @param pagePosition The position in the page to start the test at
     */
    public void putGetHelper(int bufferSize, int bufferOffset, int pagePosition) {
        putGetHelper(rwPage, rwPage, bufferSize, bufferOffset, pagePosition);
    }

    public void putGetHelper(MappedPage writer, MappedPage reader, int bufferSize, int bufferOffset, int pagePosition) {
        final int expectedSize = min(bufferSize - bufferOffset, pageSize - pagePosition);

        byte[] expected = genBytes(bufferSize);
        assertEquals(expectedSize, writer.put(pagePosition, expected, bufferOffset));

        byte[] result = new byte[bufferSize];
        System.arraycopy(result, 0, expected, 0, bufferOffset); // blank the offset values for comparison
        System.arraycopy(result, expectedSize + bufferOffset, expected , expectedSize + bufferOffset, max(bufferSize - expectedSize - bufferOffset, 0)); // blank the values past the end of the page

        assertEquals(expectedSize, reader.get(pagePosition, result, bufferOffset));
        assertArrayEquals(expected, result);
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        new Random().nextBytes(bytes);
        return bytes;
    }
}
