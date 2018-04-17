package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static org.hamcrest.core.IsInstanceOf.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileCacheTest {

    Path rootPath = Paths.get("build/test/blobs/file_cache");
    Path existingFile = rootPath.resolve("existing_file");
    Path fileDoesNotExist = rootPath.resolve("file_does_not_exist");
    Path pathDoesNotExist = rootPath.resolve("path_does_not_exist/file");

    FileCache instance;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        Files.createFile(existingFile);
    }

    @After
    public void after() {
        if (instance != null) instance.flush();
    }

    @Test
    public void testReadOnlyNonExistentFile(){
        instance = new FileCache(64, 256, true);
        assertTrue(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(fileDoesNotExist));

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        instance.getFileChannel(fileDoesNotExist);
    }

    @Test
    public void testReadOnlyNonExistentPath(){
        instance = new FileCache(64, 256, true);
        assertTrue(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(pathDoesNotExist));

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        instance.getFileChannel(pathDoesNotExist);
    }

    @Test
    public void testReadOnlyExists() throws InterruptedException {
        instance = new FileCache(64, 256, true);
        assertTrue(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(existingFile));

        FileChannel expected = instance.getFileChannel(existingFile);

        assertEquals(expected, instance.getFileChannelIfPresent(existingFile));

        assertTrue(expected.isOpen());

        instance.flush();

        // Wait for async removal listener
        Thread.sleep(100);

        assertFalse(expected.isOpen());

        assertEquals(null, instance.getFileChannelIfPresent(existingFile));
    }

    @Test
    public void testReadWriteFileDoesExists() throws InterruptedException {
        instance = new FileCache(64, 256, false);
        assertFalse(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(existingFile));

        FileChannel existingFileChannel = instance.getFileChannel(existingFile);
        assertEquals(existingFileChannel, instance.getFileChannelIfPresent(existingFile));
        assertTrue(existingFileChannel.isOpen());

        instance.flush();

        // Wait for async removal listener
        Thread.sleep(100);

        assertFalse(existingFileChannel.isOpen());

        assertEquals(null, instance.getFileChannelIfPresent(existingFile));
    }

    @Test
    public void testReadWriteFileDoesNotExist() throws InterruptedException {
        instance = new FileCache(64, 256, false);
        assertFalse(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(fileDoesNotExist));

        FileChannel newFileChannel = instance.getFileChannel(fileDoesNotExist);
        assertEquals(newFileChannel, instance.getFileChannelIfPresent(fileDoesNotExist));
        assertTrue(newFileChannel.isOpen());

        instance.flush();

        // Wait for async removal listener
        Thread.sleep(100);

        assertFalse(newFileChannel.isOpen());

        assertEquals(null, instance.getFileChannelIfPresent(fileDoesNotExist));

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        FileChannel fc = instance.getFileChannel(pathDoesNotExist);
        assertEquals(null, fc);
    }

    @Test
    public void testReadWritePathDoesNotExist() {
        instance = new FileCache(64, 256, false);
        assertFalse(instance.readOnly());

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        FileChannel fc = instance.getFileChannel(pathDoesNotExist);
        assertEquals(null, fc);
    }

    @Test
    public void testHammerFileCache(){
        instance = new FileCache(64, 256, false);

        final int requests = 1_000_000;

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(1_000_000);
        byteBuffer.flip();

        new Random()
                .ints(requests, 0, 1000)
                .parallel()
                .forEach(val -> {
                    try {
                        instance.getFileChannel(rootPath.resolve("tst" + val)).write(byteBuffer);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Could not write to tst" + val, e);
                    }
                });
        CacheStats stats = instance.stats();
        assertEquals(requests, stats.requestCount());
        assertEquals(256D/1000, stats.hitRate(), 25);
    }
}
