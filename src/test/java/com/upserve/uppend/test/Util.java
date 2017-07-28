package com.upserve.uppend.test;

import java.io.*;
import java.nio.file.*;
import java.util.Comparator;
import java.util.concurrent.atomic.*;

public class Util {
    public static void removeTempPath(Path tmpPath) throws IOException {
        if (tmpPath == null) {
            throw new NullPointerException("null temp path");
        }
        String tmpPathStr = tmpPath.toString();
        if (!(tmpPathStr.contains("tmp") || tmpPathStr.contains("temp") || tmpPathStr.startsWith("/var/folders/"))) {
            throw new IllegalArgumentException("refusing to delete temp dir that doesn't look temporary: " + tmpPathStr);
        }
        if (!Files.exists(tmpPath)) {
            return;
        }
        AtomicReference<IOException> errorRef = new AtomicReference<>();
        Files.walk(tmpPath, FileVisitOption.FOLLOW_LINKS)
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
}
