package com.upserve.uppend;

import com.google.common.primitives.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path dir;

    private static final int STRIPES = 256;
    private static final int STRIPE_MASK = STRIPES - 1;
    private static final long MAX_POSITION = 256L * 256L * 256L * 256L * 256L * 256L; // 6 Bytes allows 256TB per stripe file

    // Replace with an array
    private final FileChannel[] blobChannels;
    private final AtomicLong[] blobFilePositions;
    private final AtomicInteger nextStripe;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Blobs(Path blobDir) {
        this.dir = blobDir;

        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        blobChannels = new FileChannel[STRIPES];
        blobFilePositions = new AtomicLong[STRIPES];

        nextStripe = new AtomicInteger();

        for (int i = 0; i < STRIPES; i++) {
            Path file = dir.resolve("blobs." + String.valueOf(i));
            try {
                FileChannel chan = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                blobFilePositions[i] = new AtomicLong(chan.size());
                blobChannels[i] = chan;
            } catch (IOException e) {
                throw new UncheckedIOException("unable to init or get size of blob file: " + file, e);
            }
        }

    }

    public long append(byte[] bytes) {
        int writeSize = bytes.length + 4;
        ByteBuffer intBuf = ThreadLocalByteBuffers.LOCAL_INT_BUFFER.get();
        intBuf.putInt(bytes.length).flip();

        final int stripe = getNextStripe();

        final long stripePos = blobFilePositions[stripe].getAndAdd(writeSize);

        try {
            int writen = blobChannels[stripe].write(ByteBuffer.wrap(Bytes.concat(intBuf.array(), bytes)), stripePos);
            if (writen != writeSize) throw new IOException("Bytes written" + writen + "did not equal write size " + writeSize);
        } catch (IOException e) {
            throw new UncheckedIOException("unable write " + writeSize + " bytes @ " + stripePos + " in blob stripe: " + stripe, e);
        }

        if (stripePos > MAX_POSITION) {
            throw new RuntimeException("Max blob file size exceeded: " + stripePos);
        }

        byte[] bytePos = Longs.toByteArray(stripePos);
        bytePos[0] = (byte) stripe;
        bytePos[1] = log2Ceiling(writeSize);

        log.trace("appended {} bytes to blob stripe file {} @ {}", bytes.length, stripe, stripePos);
        return Longs.fromByteArray(bytePos);
    }

    public long size() {
        return Arrays.stream(blobFilePositions).mapToLong(AtomicLong::get).sum();
    }

    public byte[] read(long encodedLong) {
        byte[] longBytes = Longs.toByteArray(encodedLong);
        log.trace("reading from {} with {}", dir, longBytes);

        int stripe = byteToUnsignedInt(longBytes[0]);
        int sizeBound = intPowTwo(longBytes[1]);

        long pos = Longs.fromBytes((byte) 0, (byte) 0, longBytes[2], longBytes[3], longBytes[4], longBytes[5], longBytes[6], longBytes[7]);

        byte[] buf = new byte[sizeBound];

        int readSize;
        try {
            readSize = blobChannels[stripe].read(ByteBuffer.wrap(buf), pos);
            if (readSize < 4) throw new IOException("Unable to read at least 4 bytes");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read " + sizeBound + " bytes from blob stripe " + stripe + " @ " + pos, e);
        }

        int size = Ints.fromBytes(buf[0], buf[1], buf[2], buf[3]);

        if (readSize < (4 + size)) {
            throw new RuntimeException("Unable to read correct number of bytes from blob stripe" + stripe + " @ " + pos + ", read " + readSize + " but record " + size + " is greater");
        }

        final byte[] result = Arrays.copyOfRange(buf, 4, size + 4);

        log.trace("read {} bytes from blob stripe {} @ {}", size, stripe, pos);
        return result;
    }

    public void clear() {
        log.trace("clearing {}", dir);

        for (int i = 0; i < STRIPES; i++) {
            try {
                blobFilePositions[i].set(0);
                blobChannels[i].truncate(0);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to clear blob stripe : " + i, e);
            }
        }
        nextStripe.set(0);
       log.trace("clear complete {}", dir);
    }

    @Override
    public void close() {
        log.trace("closing blobs {}", dir);
        closed.set(true);
        for (int i = 0; i < STRIPES; i++) {
            try {
                blobChannels[i].close();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to close blob stripe : " + i, e);
            }
        }
        log.trace("closed blobs {}", dir);
    }

    @Override
    public void flush() {
        log.trace("flushing blobs {}", dir);

        for (int i = 0; i < STRIPES; i++) {
            try {
                blobChannels[i].force(true);
            } catch (IOException e) {
                if (closed.get()) {
                    log.debug("Unable to flush closed blobs {}", i, e);
                } else {
                    throw new UncheckedIOException("unable to flush blob stripe: " + i, e);
                }
            }
        }
        log.trace("flushed blobs {}", dir);
    }

    private byte log2Ceiling(int val) {
        if (val < 1) return 0;
        double scaledVal = Math.ceil(Math.log(val) / Math.log(2));
        return (byte) scaledVal;
    }

    private int intPowTwo(byte bval) {
        double ceilingVal = Math.min(Integer.MAX_VALUE, Math.pow(2, byteToUnsignedInt(bval)));
        return (int) ceilingVal;
    }

    private int byteToUnsignedInt(byte bval){
        int val = 0;
        val |= bval & 0xFF;
        return val;
    }

    private int getNextStripe() {
        return nextStripe.getAndIncrement() & STRIPE_MASK;
    }
}
