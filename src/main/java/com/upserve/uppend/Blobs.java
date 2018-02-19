package com.upserve.uppend;

import com.google.common.primitives.Bytes;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.atomic.*;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path file;

    private final FileChannel blobs;
    private final AtomicLong blobPosition;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean readOnly;

    public Blobs(Path file) {
        this(file, false);
    }

    public Blobs(Path file, boolean readOnly) {
        this.file = file;

        this.readOnly = readOnly;

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        OpenOption[] openOptions;
        if (readOnly) {
            openOptions = new OpenOption[]{StandardOpenOption.READ};
        } else {
            openOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }

        try {
            blobs = FileChannel.open(file, openOptions);
            blobPosition = new AtomicLong(blobs.size());
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blob file: " + file, e);
        }
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos;
        pos = blobPosition.getAndAdd(writeSize);
        try {
            ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
            intBuf.putInt(bytes.length).flip();

            blobs.write(ByteBuffer.wrap(Bytes.concat(intBuf.array(), bytes)), pos);
        } catch (IOException e) {
            throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + file, e);
        }
        log.trace("appended {} bytes to {} at pos {}", bytes.length, file, pos);
        return pos;
    }

    public long size(){
        if (readOnly) {
            try {
                return blobs.size();
            } catch (IOException e) {
                log.error("Unable to get blobs file size: " + file, e);
                return -1;
            }
        } else {
            return blobPosition.get();
        }
    }

    public byte[] read(long pos) {
        log.trace("reading from {} @ {}", file, pos);
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
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() {
        log.trace("closing {}", file);
        closed.set(true);
        try {
            blobs.close();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to close blobs " + file, e);
        }
    }

    @Override
    public void flush() {
        if (readOnly) return;
        try {
            blobs.force(true);
        } catch (IOException e) {
            if (closed.get()) {
                log.debug("Unable to flush closed blobs {}", file, e);
            } else {
                throw new UncheckedIOException("unable to flush: " + file, e);
            }
        }
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

    public static byte[] read(FileChannel chan, long pos) {
        log.trace("reading @ {}", pos);
        ByteBuffer intBuffer = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        try {
            chan.read(intBuffer, pos);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read 4 bytes at pos " + pos, e);
        }
        intBuffer.flip();
        int size = intBuffer.getInt();
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int len = buf.remaining();
        try {
            chan.read(buf, pos + 4);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read " + len + " bytes at pos " + pos, e);
        }
        log.trace("read {} bytes @ {}", size, pos);
        return bytes;

    }
}
