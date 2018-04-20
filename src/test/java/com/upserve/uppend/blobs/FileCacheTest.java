package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.hamcrest.core.IsInstanceOf.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileCacheTest {

    private final String name = "file_cache";
    private Path rootPath = Paths.get("build/test/blobs").resolve(name);
    private Path existingFile = rootPath.resolve("existing_file");
    private Path fileDoesNotExist = rootPath.resolve("file_does_not_exist");
    private Path pathDoesNotExist = rootPath.resolve("path_does_not_exist/file");

    private FileCache instance;

    private AppendOnlyStoreBuilder defaults = AppendOnlyStoreBuilder.getDefaultTestBuilder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        Files.createFile(existingFile);
    }

    @After
    public void after() throws InterruptedException {
        if (instance != null) {
            instance.flush();
            Thread.sleep(1000); // TODO fix executors for cache so we don't need this
        }
    }

    @Test
    public void testReadOnlyNonExistentFile(){
        instance = defaults.buildFileCache(true, name);
        assertTrue(instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(fileDoesNotExist));

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        instance.getFileChannel(fileDoesNotExist);
    }


    @Test
    public void testReadOnlyExists() throws InterruptedException {
        testHelper(existingFile, true);
    }

    @Test
    public void testReadWriteFileDoesExists() throws InterruptedException {
        testHelper(existingFile, false);
    }

    @Test
    public void testReadWriteFileDoesNotExist() throws InterruptedException {
        testHelper(fileDoesNotExist, false);
    }

    public void testHelper(Path path, boolean readOnly) throws InterruptedException {
        instance = defaults.buildFileCache(readOnly, name);
        assertEquals(readOnly, instance.readOnly());

        assertEquals(null, instance.getFileChannelIfPresent(path));

        FileChannel expected = instance.getFileChannel(path);

        assertEquals(expected, instance.getFileChannelIfPresent(path));

        assertTrue(expected.isOpen());

        instance.flush();

        // Wait for async removal listener
        Thread.sleep(100);

        assertFalse(expected.isOpen());

        assertEquals(null, instance.getFileChannelIfPresent(path));
    }

    @Test
    public void testReadWritePathDoesNotExist() {
        instance = defaults.buildFileCache(false, name);
        assertFalse(instance.readOnly());

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        FileChannel fc = instance.getFileChannel(pathDoesNotExist);
        assertEquals(null, fc);
    }

    @Test
    public void testHammerFileCache(){
        instance = defaults.buildFileCache(false, name);

        final int requests = 1024 * 1024;

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(10);
        byteBuffer.flip();

        AtomicInteger closedCounter = new AtomicInteger();
        new Random()
                .ints(requests, 0, 1024)
                .parallel()
                .forEach(val -> {
                    try {
                        instance.getFileChannel(rootPath.resolve("tst" + val)).write(byteBuffer);
                    } catch (ClosedChannelException e) {
                        closedCounter.addAndGet(1);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Could not write to tst" + val, e);
                    }
                });

        assertFalse("Unacceptable closed channel rate: " + closedCounter.get() , closedCounter.get() > 10);

        CacheStats stats = instance.stats();
        assertEquals(requests, stats.requestCount());
        assertEquals(0.99, stats.hitRate(), 0.05);
    }
}
