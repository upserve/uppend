package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.stats.*;
import com.google.common.primitives.Longs;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.*;

import static org.hamcrest.core.IsInstanceOf.any;
import static org.junit.Assert.*;

public class PageCacheTest {
    private final String name = "page_cache_test";
    Path rootPath = Paths.get("build/test/blobs").resolve(name);
    Path existingFile = rootPath.resolve("existing_file");
    Path fileDoesNotExist = rootPath.resolve("file_does_not_exist");
    Path pathDoesNotExist = rootPath.resolve("path_does_not_exist/file");

    ExecutorService testService = new ForkJoinPool();
    private AppendOnlyStoreBuilder defaults;

    PageCache instance;
    FileCache fileCache;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        Files.createFile(existingFile);
    }

    private void setup(boolean readOnly) {
        testService = new ForkJoinPool();

        defaults = AppendOnlyStoreBuilder
                .getDefaultTestBuilder(testService)
                .withBlobPageSize(512)
                .withInitialBlobCacheSize(128)
                .withMaximumBlobCacheSize(512);

        fileCache = defaults.buildFileCache(readOnly, name);
        instance = defaults.buildBlobPageCache(fileCache, name);
    }

    @After
    public void shutdown() throws InterruptedException {
        if (instance != null) {
            instance.flush();
        }

        if (fileCache != null){
            fileCache.flush();
        }

        if (testService != null) {
            testService.shutdown();
            testService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testGetPageFlush(){
        setup(false);

        final long position = 1284;
        FilePage page;

        page = instance.getPage(existingFile, position);
        byte[] expected = "abc".getBytes();
        page.put(0, expected, 0);

        instance.flush();

        byte[] result = new byte[3];
        page = instance.getPage(existingFile, position);
        page.get(0, result, 0);

        assertArrayEquals(expected, result);
    }

    @Test
    public void testGetPageSize(){
        setup(false);
        assertEquals(512, instance.getPageSize());
    }

    @Test
    public void testGetFileCache(){
        setup(false);

        assertEquals(fileCache, instance.getFileCache());
    }

    @Test
    public void testReadOnly() throws InterruptedException {
        setup(false);
        assertFalse(instance.readOnly());

        final long position = 1284;
        FilePage page;

        page = instance.getPage(existingFile, position);
        byte[] expected = "abc".getBytes();
        page.put(0, expected, 0);


        FileCache readOnlyFileCache = defaults.buildFileCache(true, name);
        PageCache secondInstance = defaults.buildBlobPageCache(readOnlyFileCache, name);

        assertTrue(secondInstance.readOnly());

        page = secondInstance.getPage(existingFile, position);
        byte[] result = new byte[3];
        page.get(0, result, 0);

        assertArrayEquals(expected, result);

        thrown.expect(ReadOnlyBufferException.class);

        try {
            page.put(0, result, 0); // can't put to the readonly buffer}
        } finally {
            secondInstance.flush();
            readOnlyFileCache.flush();
        }
    }

    @Test
    public void testReadOnlyNoFile() {
        setup(true);

        assertTrue(instance.readOnly());

        final long position = 1284;

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));
        instance.getPage(fileDoesNotExist, position);
    }

    @Test
    public void testHammerPageCache(){
        setup(false);

        instance.getPage(existingFile, 1000*512).flush(); // Must extend the file before concurrent writes begin

        final int requests = 1_000_000;

        new Random()
                .longs(requests, 0, 1000 * 512)
                .parallel()
                .forEach(val -> instance.getPage(existingFile, val).put(instance.pagePosition(val), Longs.toByteArray(val), 0));
        CacheStats stats = instance.stats();
        assertEquals(requests+1, stats.requestCount());
        assertEquals(256D/1000, stats.hitRate(), 25);
    }
}
