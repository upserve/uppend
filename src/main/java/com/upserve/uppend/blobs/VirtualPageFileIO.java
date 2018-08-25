package com.upserve.uppend.blobs;

import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class VirtualPageFileIO {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Supplier<ByteBuffer> LOCAL_INT_BUFFER = ThreadLocalByteBuffers.LOCAL_INT_BUFFER;
    static final Supplier<ByteBuffer> LOCAL_LONG_BUFFER = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER;

    protected final int virtualFileNumber;
    private final VirtualPageFile virtualPageFile;

    VirtualPageFileIO(int virtualFileNumber, VirtualPageFile virtualPageFile) {
        this.virtualFileNumber = virtualFileNumber;
        this.virtualPageFile = virtualPageFile;

        if (virtualFileNumber > virtualPageFile.getVirtualFiles())
            throw new IllegalStateException("Requested a virtual file " + virtualFileNumber + " which is greater than the max allocated " + virtualPageFile.getVirtualFiles());
    }

    long appendPageAlignedPosition(int size, int lowBound, int highBound) {
        // return the position for the next write
        return virtualPageFile.appendPageAlignedPosition(virtualFileNumber, size, lowBound, highBound);
    }

    long nextAlignedPosition(long position, int lowBound, int highBound) {
        return virtualPageFile.nextAlignedPosition(position, lowBound, highBound);
    }

    public boolean isReadOnly() {
        return virtualPageFile.isReadOnly();
    }

    boolean isPageAllocated(long position) {
        return virtualPageFile.isPageAvailable(virtualFileNumber, virtualPageFile.pageNumber(position));
    }

    long appendPosition(int size) {
        return virtualPageFile.appendPosition(virtualFileNumber, size);
    }

    long getPosition() {
        return virtualPageFile.getPosition(virtualFileNumber);
    }

    void writeInt(long pos, int val) {
        write(pos, int2bytes(val));
    }

    static byte[] int2bytes(int val) {
        ByteBuffer intBuf = LOCAL_INT_BUFFER.get();
        intBuf.putInt(val).flip();
        return intBuf.array();
    }

    void writeLong(long pos, long val) {
        write(pos, long2bytes(val));
    }

    static byte[] long2bytes(long val) {
        ByteBuffer longBuf = LOCAL_LONG_BUFFER.get();
        longBuf.putLong(val).flip();
        return longBuf.array();
    }

    void write(long pos, byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Can not write empty bytes!");
        }
        final int result = writePagedOffset(pos, bytes, 0);
        if (result != bytes.length) {
            throw new RuntimeException("Failed to write all the bytes: " + bytes.length + " != " + result);
        }
    }

    private int writePagedOffset(long pos, byte[] bytes, int offset) {
        int pageNumber = virtualPageFile.pageNumber(pos);

        Page page = virtualPageFile.getOrCreatePage(virtualFileNumber, pageNumber);

        int bytesWritten;
        bytesWritten = page.put(virtualPageFile.pagePosition(pos), bytes, offset);

        if (bytesWritten < (bytes.length - offset)) {
            bytesWritten += writePagedOffset(pos + bytesWritten, bytes, offset + bytesWritten);
        }
        return bytesWritten;
    }

    int readInt(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[4];
        read(pos, buf);
        return Ints.fromByteArray(buf);
    }

    long readLong(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[8];
        read(pos, buf);
        return Longs.fromByteArray(buf);
    }

    void read(long pos, byte[] buf) {
        if (buf.length == 0) return;
        final int result = readPagedOffset(pos, buf, 0);
        if (result != buf.length) {
            throw new IllegalStateException("Unable to read requested bytes");
        }
    }

    private int readPagedOffset(long pos, byte[] buf, int offset) {
        int pageNumber = virtualPageFile.pageNumber(pos);

        Page page = virtualPageFile.getExistingPage(virtualFileNumber, pageNumber);

        int bytesRead;
        bytesRead = page.get(virtualPageFile.pagePosition(pos), buf, offset);

        if (bytesRead < (buf.length - offset)) {
            // TODO see if it is faster to use the head pointer to the next page start rather than recursion here?
            bytesRead += readPagedOffset(pos + bytesRead, buf, offset + bytesRead);
        }
        return bytesRead;
    }
}
