package com.upserve.uppend.util;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

public class SafeDeleting {
    private static void validatePathForRemoval(Path path) throws IOException {
        if (path == null || path.toFile().getAbsolutePath().length() < 4) {
            throw new IOException("refusing to delete null or short path: " + path);
        }
    }

    private static void unsafeRemovePath(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        AtomicReference<IOException> errorRef = new AtomicReference<>();
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    if (errorRef.get() != null) {
                        return;
                    }
                    try {
                        Files.delete(p);
                    } catch (IOException ioe) {
                        errorRef.set(new IOException("unable to delete: " + p, ioe));
                    }
                });
        IOException error = errorRef.get();
        if (error != null) {
            throw error;
        }
    }

    public static void removeTempPath(Path tmpPath) throws IOException {
        validatePathForRemoval(tmpPath);
        String tmpPathStr = tmpPath.toString();
        if (!(tmpPathStr.contains("tmp") || tmpPathStr.contains("temp") || tmpPathStr.startsWith("/var/folders/"))) {
            throw new IllegalArgumentException("refusing to delete temp dir that doesn't look temporary: " + tmpPathStr);
        }
        unsafeRemovePath(tmpPath);
    }

    public static void removeDirectory(Path path) throws IOException {
        validatePathForRemoval(path);
        unsafeRemovePath(path);
    }
}
