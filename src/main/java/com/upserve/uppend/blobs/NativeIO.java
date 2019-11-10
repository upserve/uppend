package com.upserve.uppend.blobs;

import org.slf4j.Logger;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.MappedByteBuffer;

public class NativeIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public enum Advice {
        Normal(0), Sequential(1), Random(2), WillNeed(3), DontNeed(4);
        private final int value;
        Advice(int val) {
            this.value = val;
        }
    }

    public native void madvise(MappedByteBuffer buffer, Advice advise) throws IOException;

    static {
        log.info("loading nativeIO libbrary");
        System.loadLibrary("nativeIO");
    }
}
