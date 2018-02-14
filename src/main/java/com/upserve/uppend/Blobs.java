package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.stream.IntStream;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path file;

    private static final int lengthGuess = 1024;

    private static final int stripes = 32;
    private final ConcurrentHashMap<Integer, Map.Entry<AtomicLong, FileChannel>> blobChannels;

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


        blobChannels = new ConcurrentHashMap<>();

        blobPosition = new AtomicLong(IntStream.range(0, stripes).mapToLong(val -> {
            try {
                FileChannel chan = FileChannel.open(file.resolveSibling("blobs." + String.valueOf(val)), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                blobChannels.put(val, Maps.immutableEntry(new AtomicLong(chan.size()), chan));
                return chan.size();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to init blob file: " + file, e);
            }

        }).sum());

    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        final long pos;
        pos = blobPosition.getAndAdd(writeSize);

        ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        intBuf.putInt(bytes.length).flip();

        int stripe = (int) (pos % stripes);

        final long stripePos;

        try {
            Map.Entry<AtomicLong, FileChannel> entry = blobChannels.get(stripe);

            stripePos = entry.getKey().getAndAdd(writeSize);

            entry.getValue().write(ByteBuffer.wrap(Bytes.concat(intBuf.array(), bytes)), stripePos);
        } catch (IOException e) {
            throw new UncheckedIOException("unable write " + writeSize + " bytes at position " + pos + ": " + file, e);
        }
        // If stripe pos is > 256**7 this will be bad - but that is a big number...
        byte[] bytePos = Longs.toByteArray(stripePos);
        bytePos[0] = (byte) stripe;

        log.trace("appended {} bytes to {} at pos {}", bytes.length, file, pos);
        return Longs.fromByteArray(bytePos);
    }

    public long size() {
        return blobPosition.get();
    }

    public byte[] read(long pos) {
        log.trace("reading from {} @ {}", file, pos);
        byte[] buf = new byte[lengthGuess + 4];
        read(pos, buf);

        int size = Ints.fromBytes(buf[0],buf[1],buf[2], buf[3]);

        final byte[] result;
        if (size < lengthGuess){
            result = Arrays.copyOfRange(buf, 4, size + 4);
        } else {

            buf = new byte[size + 4];
            read(pos, buf);

            result = Arrays.copyOfRange(buf, 4, size + 4);
        }

        log.trace("read {} bytes from {} @ {}", size, file, pos);
        return result;
    }

    public void clear() {
        log.trace("clearing {}", file);
        blobChannels.values().forEach(entry -> {
            try {
                entry.getValue().truncate(0);
                entry.getKey().set(0);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to clear", e);
            }
        });
        blobPosition.set(0);
    }

    @Override
    public void close() {
        log.trace("closing {}", file);
        closed.set(true);
        blobChannels.values().forEach(entry -> {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to close blobs " + file, e);
            }
        });

    }

    @Override
    public void flush() {

        blobChannels.values().forEach(entry -> {
            try {
                entry.getValue().force(true);
            } catch (IOException e) {
                if (closed.get()) {
                    log.debug("Unable to flush closed blobs {}", file, e);
                } else {
                    throw new UncheckedIOException("unable to flush: " + file, e);
                }
            }
        });
    }

    private void read(long pos, byte[] buf) {
        read(pos, ByteBuffer.wrap(buf));
    }

    private void read(long pos, ByteBuffer buf) {
        int len = buf.remaining();

        byte[] longBytes = Longs.toByteArray(pos);
        int stripe = longBytes[0];

        longBytes[0] = 0;

        long stripePos = Longs.fromByteArray(longBytes);


        try {
            Map.Entry<AtomicLong, FileChannel> entry = blobChannels.get(stripe);

            entry.getValue().read(buf, stripePos);
        } catch (IOException e) {
            throw new UncheckedIOException("Reading " + pos + ": " + file, e);
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
