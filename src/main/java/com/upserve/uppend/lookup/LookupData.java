package com.upserve.uppend.lookup;

import com.upserve.uppend.AutoFlusher;
import com.upserve.uppend.util.*;
import it.unimi.dsi.fastutil.objects.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.*;

@Slf4j
public class LookupData implements AutoCloseable, Flushable {

    public static final AtomicLong memKeyCount = new AtomicLong();

    private final AtomicBoolean isClosed;

    private final int keyLength;
    private final Path path;
    private final Path metadataPath;

    private FileChannel chan;
    private DataOutputStream out;

    private Object2LongSortedMap<LookupKey> mem;
    private Object2IntLinkedOpenHashMap<LookupKey> memOrder;

    public LookupData(int keyLength, Path path, Path metadataPath, int flushDelaySeconds) {
        log.trace("opening lookup data: {}", path);
        this.keyLength = keyLength;

        this.path = path;
        this.metadataPath = metadataPath;
        Path parentPath = path.getParent();
        if (Files.notExists(parentPath)) {
            try {
                Files.createDirectories(parentPath);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to make parent dir: " + parentPath, e);
            }
        }

        try {
            chan = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            chan.position(chan.size());
        } catch (IOException e) {
            throw new UncheckedIOException("can't open file: " + path, e);
        }
        out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(chan), 8192));

        try {
            init();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to initialize data at " + path, e);
        }

        AutoFlusher.register(flushDelaySeconds, this);
        isClosed = new AtomicBoolean(false);
    }

    public synchronized void put(LookupKey key, long value) {
        log.trace("putting {}={} in {}", key, value, path);
        long existingValue = mem.put(key, value);
        if (existingValue != Long.MIN_VALUE) {
            throw new IllegalStateException("can't put same key ('" + key + "') twice: new value = " + value + ", existing value = " + existingValue);
        }
        int pos = memOrder.size();
        if (memOrder.put(key, pos) != Integer.MIN_VALUE) {
            throw new IllegalStateException("encountered repeated mem order key at pos " + pos + ": " + key);
        }
        append(key, value);
    }

    public synchronized long putIfNotExists(LookupKey key, long value) {
        log.trace("putting (if not exists) {}={} in {}", key, value, path);
        long existingValue = mem.put(key, value);
        if (existingValue != Long.MIN_VALUE) {
            return existingValue;
        }
        int pos = memOrder.size();
        if (memOrder.put(key, pos) != Integer.MIN_VALUE) {
            throw new IllegalStateException("encountered repeated mem order key at pos " + pos + ": " + key);
        }
        append(key, value);
        return value;
    }

    public synchronized long putIfNotExists(LookupKey key, LongSupplier allocateLongFunc) {
        log.trace("putting (if not exists) {}=<lambda> in {}", key, path);

        long firstExistingValue = mem.getLong(key);
        if (firstExistingValue != Long.MIN_VALUE) {
            return firstExistingValue;
        }

        long newValue = allocateLongFunc.getAsLong();
        long existingValue = putIfNotExists(key, newValue);
        if (existingValue != newValue) {
            log.warn("lost race to allocate, wasted new value " + newValue + " for key: " + key);
        }
        return existingValue;
    }

    private void append(LookupKey key, long value) {
        byte[] keyBytes = key.bytes();
        if (keyBytes.length != keyLength) {
            throw new IllegalStateException("unexpected key length: expected " + keyLength + ", got " + keyBytes.length);
        }
        try {
            out.write(keyBytes);
            out.writeLong(value);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write key: " + key, e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        log.trace("closing lookup data at {} (~{} entries)", path, mem.size());
        if (isClosed.compareAndSet(false, true)) {
            AutoFlusher.deregister(this);
            flush();
            out.close();
            log.trace("closed lookup data at {}", path);
        } else {
            log.warn("lookup data already closed: " + path, new RuntimeException("was closed") /* get stack */);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        log.trace("flushing lookup and metadata at {}", metadataPath);
        memKeyCount.addAndGet(mem.size());
        out.flush();
        LookupMetadata metadata = generateMetadata();
        metadata.writeTo(metadataPath);
        log.trace("flushed lookup and metadata at {}: {}", metadataPath, metadata);
    }

    private synchronized void init() throws IOException {
        mem = new Object2LongAVLTreeMap<>();
        mem.defaultReturnValue(Long.MIN_VALUE);

        memOrder = new Object2IntLinkedOpenHashMap<>();
        memOrder.defaultReturnValue(Integer.MIN_VALUE);

        chan.position(0);
        long pos = 0;
        long size = chan.size();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(Channels.newInputStream(chan), 8192));
        // don't call dis.close() on wrapping InputStream since we don't want it to chain to chan.close()
        while (pos < size) {
            long nextPos = pos + keyLength + 8;
            if (nextPos > size) {
                // corrupt; fix
                log.error("truncating at pos " + pos + " for file of corrupted size " + size + " with key length " + keyLength);
                chan.truncate(pos);
                break;
            }
            byte[] keyBytes = new byte[keyLength];
            try {
                dis.readFully(keyBytes);
            } catch (EOFException e) {
                throw new IOException("got eof at pos " + pos + " while trying to read " + keyLength + " bytes", e);
            }
            LookupKey key = new LookupKey(keyBytes);
            long val;
            try {
                val = dis.readLong();
            } catch (IOException e) {
                throw new IOException("read bad value for key " + key + " at pos " + pos, e);
            }
            if (mem.put(key, val) != Long.MIN_VALUE) {
                throw new IllegalStateException("encountered repeated mem key at pos " + pos + ": " + key);
            }
            if (memOrder.put(key, memOrder.size()) != Integer.MIN_VALUE) {
                throw new IllegalStateException("encountered repeated mem order key at pos " + pos + ": " + key);
            }
            pos = nextPos;
        }
        if (chan.position() != chan.size()) {
            log.warn("scan incomplete at pos " + chan.position() + " / " + chan.size());
        }
    }

    private synchronized LookupMetadata generateMetadata() {
        LookupKey minKey = mem.firstKey();
        LookupKey maxKey = mem.lastKey();
        int[] keyStorageOrder = memOrder.values().toIntArray();
        String[] keyStrings = memOrder.keySet().stream().map(LookupKey::toString).toArray(String[]::new);
        IntArrayCustomSort.sort(keyStorageOrder, (a, b) -> keyStrings[a].compareTo(keyStrings[b]));
        return new LookupMetadata(
                keyLength,
                mem.size(),
                minKey,
                maxKey,
                keyStorageOrder
        );
    }

    static Stream<LookupKey> keys(Path path, int keyLength) {
        KeyIterator iter;
        try {
            iter = new KeyIterator(path, keyLength);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to create key iterator for path: " + path, e);
        }
        Spliterator<LookupKey> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true);
    }

    private static class KeyIterator implements Iterator<LookupKey> {
        private final Path path;
        private final int keyLength;
        private final FileChannel chan;
        private final int numKeys;
        private int keyIndex = 0;

        public KeyIterator(Path path, int keyLength) throws IOException {
            this.path = path;
            this.keyLength = keyLength;
            chan = FileChannel.open(path, StandardOpenOption.READ);
            numKeys = (int) (chan.size() / (keyLength + 8));
        }

        public int getNumKeys() {
            return numKeys;
        }

        @Override
        public boolean hasNext() {
            return keyIndex < numKeys;
        }

        @Override
        public LookupKey next() {
            LookupKey key;
            try {
                key = readKey(chan, keyLength, keyIndex);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to read at key index " + keyIndex + " from " + path, e);
            }
            keyIndex++;
            return key;
        }
    }

    static LookupKey readKey(FileChannel chan, int keyLength, int keyNumber) throws IOException {
        byte[] keyBytes = new byte[keyLength];
        ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes);
        long pos = keyNumber * (keyLength + 8);
        chan.read(keyBuf, pos);
        return new LookupKey(keyBytes);
    }

    static long readValue(FileChannel chan, int keyLength, int keyIndex) throws IOException {
        long pos = keyIndex * (keyLength + 8) + keyLength;
        ByteBuffer buf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
        chan.read(buf, pos);
        buf.flip();
        return buf.getLong();
    }
}
