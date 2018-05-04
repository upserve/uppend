package com.upserve.uppend.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SafeDeletingTest {
    @Test
    public void testRemoveNonexistantDirectory() throws IOException {
        SafeDeleting.removeDirectory(Paths.get("/SDKJfnaw98rja9w8efjiausdhf"));
        SafeDeleting.removeDirectory(Paths.get("_f-")); // short, but relative
    }

    @Test(expected = IOException.class)
    public void testRemoveShortDirectory() throws IOException {
        SafeDeleting.removeDirectory(Paths.get("/z_"));
    }

    @Test
    public void testRemoveExistingDirectory() throws IOException {
        Path dir = Paths.get("build/test/safe-deleting/existing-dir");
        Files.createDirectories(dir);
        assertTrue(Files.exists(dir));
        SafeDeleting.removeDirectory(dir);
        assertFalse(Files.exists(dir));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveTempPathWithoutTemporaryName() throws IOException {
        Path dir = Paths.get("build/test/safe-deleting/not-t-e-m-p-o-r-a-r-y");
        Files.createDirectories(dir);
        assertTrue(Files.exists(dir));
        SafeDeleting.removeTempPath(dir);
    }

    @Test
    public void testRemoveTempPath() throws IOException {
        Path dir = Files.createTempDirectory("safe-deleting-test");
        assertTrue(Files.exists(dir));
        SafeDeleting.removeTempPath(dir);
        assertFalse(Files.exists(dir));
    }
}
