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

    VirtualPageFile virtualPageFile;
    PageCache instance;

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

        instance = defaults.buildBlobPageCache(name);

        virtualPageFile = new VirtualPageFile(existingFile, 12, false, instance);
    }

    @After
    public void shutdown() throws InterruptedException {
        if (instance != null) {
            instance.flush();
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
        Page page;

        page = instance.get(virtualPageFile, position);
        byte[] expected = "abc".getBytes();
        page.put(0, expected, 0);

        instance.flush();

        byte[] result = new byte[3];
        page = instance.get(virtualPageFile, position);
        page.get(0, result, 0);

        assertArrayEquals(expected, result);
    }

    @Test
    public void testGetPageSize(){
        setup(false);
        assertEquals(512, instance.getPageSize());
    }
}
