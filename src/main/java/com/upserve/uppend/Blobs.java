package com.upserve.uppend;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.atomic.*;

@Slf4j
public class Blobs implements AutoCloseable {
    private static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;

    private final Path file;
    private final int flushDelaySeconds;

    private final FileChannel blobs;
    private final AtomicLong blobPosition;
    private final Object appendMonitor = new Object();
    private DataOutputStream out;
    private final AtomicBoolean outDirty;

    public Blobs(Path file) {
        this(file, DEFAULT_FLUSH_DELAY_SECONDS);
    }

    public Blobs(Path file, int flushDelaySeconds) {
        this.file = file;
        this.flushDelaySeconds = flushDelaySeconds;

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        try {
            blobs = FileChannel.open(dir.resolve("blobs"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            blobs.position(blobs.size());
            out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(blobs)));
            blobPosition = new AtomicLong(blobs.size());
            AutoFlusher.register(flushDelaySeconds, out);
            outDirty = new AtomicBoolean(false);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blob file: " + dir + "/blobs", e);
        }
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        long pos;
        synchronized (appendMonitor) {
            pos = blobPosition.getAndAdd(writeSize);
            try {
                out.writeInt(bytes.length);
                out.write(bytes);
            } catch (IOException e) {
                throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + file, e);
            }
            outDirty.set(true);
        }
        log.trace("appended {} bytes to {} at pos {}", bytes.length, file, pos);
        return pos;
    }

    public byte[] read(long pos) {
        log.trace("reading from {} @ {}", file, pos);
        if (outDirty.get()) {
            try {
                synchronized (appendMonitor) {
                    out.flush();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("unable to flush dirty output: " + file, e);
            }
            outDirty.set(false);
        }
        int size = readInt(pos);
        byte[] buf = new byte[size];
        read(pos + 4, buf);
        log.trace("read {} bytes from {} @ {}", size, file, pos);
        return buf;
    }

    public void clear() {
        log.trace("clearing {}", file);
        try {
            blobs.truncate(0);
            blobPosition.set(0);
            AutoFlusher.deregister(out);
            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file.toFile())));
            AutoFlusher.register(flushDelaySeconds, out);
            outDirty.set(false);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() throws Exception {
        log.trace("closing {}", file);
        synchronized (appendMonitor) {
            out.close();
            AutoFlusher.deregister(out);
            outDirty.set(false);
        }
        blobs.close();
    }

    private int readInt(long pos) {
        ByteBuffer intBuffer = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        read(pos, intBuffer);
        intBuffer.flip();
        return intBuffer.getInt();
    }

    private void read(long pos, byte[] buf) {
        read(pos, ByteBuffer.wrap(buf));
    }

    private void read(long pos, ByteBuffer buf) {
        int len = buf.remaining();
        try {
            blobs.read(buf, pos);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read " + len + " bytes at pos " + pos + " in " + file, e);
        }
    }
}
