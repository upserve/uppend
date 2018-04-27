package com.upserve.uppend.blobs;

import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.*;
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

        if (virtualFileNumber > virtualPageFile.getVirtualFiles()) throw new IllegalStateException("Requested a virtual file " + virtualFileNumber + " which is greater than the max allocated " + virtualPageFile.getVirtualFiles());
    }

    long appendPosition(int size) {
        // return the position to write at
        final long result = virtualPageFile.getAtomicVirtualFilePosition(virtualFileNumber).getAndAdd(size);
        // record the position written too
        virtualPageFile.putHeaderVirtualFilePosition(virtualFileNumber,result+size);
        // It is possible to have a race here which could result in loosing an appended value if the writer process dies
        // before it writes again...
        return result;
    }

    public long getPosition(){
        if (virtualPageFile.isReadOnly()){
            return virtualPageFile.getHeaderVirtualFilePosition(virtualFileNumber);
        } else{
            return virtualPageFile.getAtomicVirtualFilePosition(virtualFileNumber).get();
        }
    }

    void writeMappedInt(long pos, int val){
        writeMapped(pos, int2bytes(val));
    }

    static byte[] int2bytes(int val){
        ByteBuffer intBuf = LOCAL_INT_BUFFER.get();
        intBuf.putInt(val).flip();
        return intBuf.array();
    }

    void writeMappedLong(long pos, long val){
        writeMapped(pos, long2bytes(val));
    }

    static byte[] long2bytes(long val){
        ByteBuffer longBuf = LOCAL_LONG_BUFFER.get();
        longBuf.putLong(val).flip();
        return longBuf.array();
    }

    void writeMapped(long pos, byte[] bytes){
        if (bytes.length == 0) {
            return;
        }
        final int result = writeMappedOffset(pos, bytes, 0);
        if (result != bytes.length) {
            throw new RuntimeException("Failed to write all the bytes: " + bytes.length + " != " + result);
        }
    }

    private int writeMappedOffset(long pos, byte[] bytes, int offset) {
        int pageNumber = virtualPageFile.pageNumber(pos);
        Page page = virtualPageFile.getPage(virtualFileNumber, pageNumber);

        int bytesWritten;
        bytesWritten = page.put(virtualPageFile.pagePosition(pos), bytes, offset);

        if (bytesWritten < (bytes.length - offset)){
            bytesWritten += writeMappedOffset(pos + bytesWritten, bytes, offset + bytesWritten);
        }
        return bytesWritten;
    }

    int readMappedInt(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[4];
        readMapped(pos, buf);
        return Ints.fromByteArray(buf);
    }

    long readMappedLong(long pos) {
        // TODO make thread local byte array?
        byte[] buf = new byte[8];
        readMapped(pos, buf);
        return Longs.fromByteArray(buf);
    }

    void readMapped(long pos, byte[] buf){
        if (buf.length == 0) return;
        final int result = readMappedOffset(pos, buf, 0);
        if (result != buf.length) {
            throw new RuntimeException("FOo");
        }
    }

    private int readMappedOffset(long pos, byte[] buf, int offset) {
        int pageNumber = virtualPageFile.pageNumber(pos);
        Page page = virtualPageFile.getMappedPage(virtualFileNumber, pageNumber);

        int bytesRead;
        bytesRead = page.get(virtualPageFile.pagePosition(pos), buf, offset);

        if (bytesRead < (buf.length - offset)){
            bytesRead += readMappedOffset(pos + bytesRead, buf, offset + bytesRead);
        }
        return bytesRead;
    }

}
