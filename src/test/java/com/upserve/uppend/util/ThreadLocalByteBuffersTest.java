package com.upserve.uppend.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ThreadLocalByteBuffersTest {
    @Test
    public void testDifferentThreadsGetDifferentBuffers() {
        ThreadLocalByteBuffers.threadLocalByteBufferSupplier(1);
    }

    @Test
    public void testSameThreadGetsSameBuffer() {
        ByteBuffer buf1 = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(1).get();
        ByteBuffer buf2 = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(1).get();
        assertEquals(buf1, buf2);
    }

    @Test
    public void testBufferIsReset() {
        ByteBuffer buf;
        for (int i = 0; i < 5; i++) {
            buf = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(1).get();
            assertEquals(1, buf.remaining());
            buf.put((byte) 0);
            assertEquals(0, buf.remaining());
        }
    }
}
