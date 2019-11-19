package com.upserve.uppend.blobs;

import jnr.ffi.*;
import jnr.ffi.types.size_t;
import org.slf4j.Logger;
import com.kenai.jffi.MemoryIO;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.*;

public class NativeIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final NativeC nativeC = LibraryLoader.create(NativeC.class).load("c");
    public static final int pageSize = nativeC.getpagesize(); // 4096 on most Linux

    public enum Advice {
        // These seem to be fairly stable https://github.com/torvalds/linux
        // TODO add to https://github.com/jnr/jnr-constants
        Normal(0), Random(1), Sequential(2), WillNeed(3), DontNeed(4);
        private final int value;
        Advice(int val) {
            this.value = val;
        }
    }

    public interface NativeC {
        int madvise(@size_t long address, @size_t long size, int advice);
        int getpagesize();
    }

    static long alignedAddress(long address) {
        return address & (- pageSize);
    }

    static long alignedSize(long address, int capacity) {
        long end = address + capacity;
        end = (end + pageSize - 1) & (-pageSize);
        return end - alignedAddress(address);
    }

    public static void madvise(MappedByteBuffer buffer, Advice advice) throws IOException {

        final long address = MemoryIO.getInstance().getDirectBufferAddress(buffer);
        final int capacity = buffer.capacity();

        long alignedAddress = alignedAddress(address);
        long alignedSize = alignedSize(alignedAddress, capacity);

        log.debug(
                "Page size {}; Address: raw - {}, aligned - {}; Size: raw - {}, aligned - {}",
                pageSize, address, alignedAddress, capacity, alignedSize
        );
        int val = nativeC.madvise(alignedAddress, alignedSize, advice.value);

        if (val != 0) {
            throw new IOException(String.format("System call madvise failed with code: %d", val));
        }
    }
}
