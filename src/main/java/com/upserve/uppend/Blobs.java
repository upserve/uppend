package com.upserve.uppend;

import com.google.common.primitives.Bytes;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path file;

    private static final long stripes = 64;
    private final ConcurrentHashMap<Integer, FileChannel> blobChannels;

    private final AtomicLong blobPosition;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Blobs(Path file) {
        this.file = file;

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        try {

            blobChannels = new ConcurrentHashMap<>();

            blobChannels.put(0, FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));

            blobPosition = new AtomicLong(blobChannels.get(0).size());
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blob file: " + file, e);
        }
    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos;
        pos = blobPosition.getAndAdd(writeSize);

        ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        intBuf.putInt(bytes.length).flip();

        int stripe = (int) (pos % stripes);

        blobChannels.compute(stripe, (key, blobs) -> {
            if (blobs == null) {
                try {
                    blobs = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to open: " + file, e);
                }
            }

            try {
                blobs.write(ByteBuffer.wrap(Bytes.concat(intBuf.array(), bytes)), pos);
            } catch (IOException e) {
                throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + file, e);                }

            return blobs;

        });

        log.trace("appended {} bytes to {} at pos {}", bytes.length, file, pos);
        return pos;
    }

    public long size(){
        return blobPosition.get();
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
            blobChannels.get(0).truncate(0);
            blobPosition.set(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear", e);
        }
    }

    @Override
    public void close() {
        log.trace("closing {}", file);
        closed.set(true);
        blobChannels.values().stream().forEach(blobs -> {
            try {
                blobs.close();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to close blobs " + file, e);
            }
        });

    }

    @Override
    public void flush() {

        blobChannels.values().stream().forEach(blobs -> {
            try {
                blobs.force(true);
            } catch (IOException e) {
                if (closed.get()) {
                    log.debug("Unable to flush closed blobs {}", file, e);
                } else {
                    throw new UncheckedIOException("unable to flush: " + file, e);
                }
            }
        });
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

        int stripe = (int) (pos % stripes);


        blobChannels.compute(stripe, (key, blobs) -> {
            if (blobs == null) {
                try {
                    blobs = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to open: " + file, e);
                }
            }

            try {
                blobs.read(buf, pos);
            } catch (IOException e) {
                throw new UncheckedIOException("Reading " + pos + ": " + file, e);
            }

            return blobs;

        });
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
