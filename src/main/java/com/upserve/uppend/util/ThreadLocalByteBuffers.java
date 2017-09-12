package com.upserve.uppend.util;

import it.unimi.dsi.fastutil.ints.*;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class ThreadLocalByteBuffers {
    private static final Int2ObjectMap<Supplier<ByteBuffer>> localBuffersBySize = new Int2ObjectOpenHashMap<>();

    public static final Supplier<ByteBuffer> LOCAL_INT_BUFFER = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(4);

    public static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(8);

    public synchronized static Supplier<ByteBuffer> threadLocalByteBufferSupplier(int size) {
        Supplier<ByteBuffer> localBuffer = localBuffersBySize.get(size);
        if (localBuffer == null) {
            localBuffer = threadLocalByteBufferSupplier(() -> ByteBuffer.allocate(size));
            localBuffersBySize.put(size, localBuffer);
        }
        return localBuffer;
    }

    public static Supplier<ByteBuffer> threadLocalByteBufferSupplier(Supplier<ByteBuffer> allocator) {
        return new BufferSupplier(allocator);
    }

    private static class BufferSupplier implements Supplier<ByteBuffer> {
        private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();
        private final Supplier<ByteBuffer> allocator;

        private BufferSupplier(Supplier<ByteBuffer> allocator) {
            this.allocator = allocator;
        }

        @Override
        public ByteBuffer get() {
            ByteBuffer buf = localBuffer.get();
            if (buf == null) {
                buf = allocator.get();
                localBuffer.set(buf);
            } else {
                buf.clear();
            }
            return buf;
        }
    }
}
