package com.upserve.uppend;

import com.upserve.uppend.cli.Cli;

import java.io.*;
import java.nio.file.*;

public final class Uppend {
    public static final String VERSION;

    static {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("com/upserve/uppend/main.properties")) {
            System.getProperties().load(is);
        } catch (IOException e) {
            System.err.println("WARNING: unable to load com/upserve/uppend/main.properties: " + e.getMessage());
        }
        VERSION = System.getProperty("uppend.version", "unknown");
    }

    private Uppend() {
    }

    public static FileAppendOnlyStoreBuilder store(String path) {
        return store(Paths.get(path));
    }

    public static FileAppendOnlyStoreBuilder store(Path path) {
        return new FileAppendOnlyStoreBuilder().withDir(path);
    }

    public static FileCounterStoreBuilder counterStore(String path) {
        return counterStore(Paths.get(path));
    }

    public static FileCounterStoreBuilder counterStore(Path path) {
        return new FileCounterStoreBuilder().withDir(path);
    }


    public static void main(String ... args) throws Exception {
        Cli.main(args);
    }
}
