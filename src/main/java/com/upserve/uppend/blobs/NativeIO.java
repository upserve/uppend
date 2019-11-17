package com.upserve.uppend.blobs;

import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;

public class NativeIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public enum Advice {
        Normal(0), Sequential(1), Random(2), WillNeed(3), DontNeed(4);
        private final int value;
        Advice(int val) {
            this.value = val;
        }
    }

    public static native void madvise(MappedByteBuffer buffer, Advice advise) throws IOException;

    static {
        log.info("loading nativeIO libbrary...");

        try{
            System.loadLibrary("nativeIO");
            log.info("System.loadLibrary(\"nativeIO\") - successful!");
        } catch (UnsatisfiedLinkError e) {

            String libName = "libnativeIO.dylib"; // The name of the file in resources/ dir
            URL url = NativeIO.class.getResource("/" + libName);
            File tmpDir = null;
            try {
                tmpDir = Files.createTempDirectory("nativeIO-lib").toFile();
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not create tmpdir for nativeIO library", ex);
            }
            tmpDir.deleteOnExit();
            File nativeLibTmpFile = new File(tmpDir, libName);
            nativeLibTmpFile.deleteOnExit();
            try (InputStream in = url.openStream()) {
                Files.copy(in, nativeLibTmpFile.toPath());
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not copy libnativeIO", ex);
            }
            System.load(nativeLibTmpFile.getAbsolutePath());
            log.info("System.load(nativeLibTmpFile.getAbsolutePath()); - successful!");
        }
    }
}
