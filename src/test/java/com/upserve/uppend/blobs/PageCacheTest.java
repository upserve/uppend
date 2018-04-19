package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.primitives.Longs;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static org.hamcrest.core.IsInstanceOf.any;
import static org.junit.Assert.*;

public class PageCacheTest {
    Path rootPath = Paths.get("build/test/blobs/page_cache");
    Path existingFile = rootPath.resolve("existing_file");
    Path fileDoesNotExist = rootPath.resolve("file_does_not_exist");
    Path pathDoesNotExist = rootPath.resolve("path_does_not_exist/file");

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

    @After
    public void after() throws InterruptedException {
        if (instance != null) {
            instance.flush();
            Thread.sleep(100); // TODO fix executors for cache so we don't need this
        }
    }

    @Test
    public void testGetPageFlush(){
        fileCache = new FileCache(64, 256, false);
        instance = new PageCache(512, 128, 512, fileCache);

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
        fileCache = new FileCache(64, 256, false);
        instance = new PageCache(512, 128, 512, fileCache);

        assertEquals(512, instance.getPageSize());
    }

    @Test
    public void testGetFileCache(){
        fileCache = new FileCache(64, 256, false);
        instance = new PageCache(512, 128, 512, fileCache);

        assertEquals(fileCache, instance.getFileCache());
    }

    @Test
    public void testReadOnly(){
        fileCache = new FileCache(64, 256, false);
        instance = new PageCache(512, 128, 512, fileCache);

        assertFalse(instance.readOnly());

        final long position = 1284;
        FilePage page;

        page = instance.getPage(existingFile, position);
        byte[] expected = "abc".getBytes();
        page.put(0, expected, 0);


        fileCache = new FileCache(64, 256, true);
        instance = new PageCache(512, 128, 512, fileCache);

        assertTrue(instance.readOnly());

        page = instance.getPage(existingFile, position);
        byte[] result = new byte[3];
        page.get(0, result, 0);

        assertArrayEquals(expected, result);

        thrown.expect(ReadOnlyBufferException.class);
        page.put(0, result, 0); // can't put to the readonly buffer
    }

    @Test
    public void testReadOnlyNoFile() {
        fileCache = new FileCache(64, 256, true);
        instance = new PageCache(512, 128, 512, fileCache);

        assertTrue(instance.readOnly());

        final long position = 1284;

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));
        instance.getPage(fileDoesNotExist, position);
    }

    @Test
    public void testHammerPageCache(){
        fileCache = new FileCache(64, 256, false);
        instance = new PageCache(512, 128, 256, fileCache);

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
