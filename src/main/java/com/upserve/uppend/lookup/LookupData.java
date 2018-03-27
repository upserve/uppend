package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.Blobs;

import com.upserve.uppend.util.*;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public class LookupData implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicBoolean isClosed;

    private final Path path;
    private final Path metadataPath;
    private final Path keysPath;
    private final Path blobsPath;
    private final Supplier<ByteBuffer> recordBufSupplier;
    private final FileChannel chan;
    private final FileChannel keys;
    private final AtomicInteger keysPos;

    private final Blobs blobs;

    private final Object memMonitor = new Object();
    private Object2LongSortedMap<LookupKey> mem;
    private Object2IntLinkedOpenHashMap<LookupKey> memOrder;

    private final AtomicBoolean isDirty = new AtomicBoolean(false);


    public LookupData(Path path, Path metadataPath) {
        log.trace("opening lookup data: {}", path);

        this.path = path;
        this.metadataPath = metadataPath;
        keysPath = path.resolveSibling("keys");
        blobsPath = path.resolveSibling("blobs");

        Path parentPath = path.getParent();
        if (!Files.exists(parentPath) /* notExists() returns true erroneously */) {
            try {
                Files.createDirectories(parentPath);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to make parent dir: " + parentPath, e);
            }
        }

        recordBufSupplier = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(16);

        try {
            chan = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException("can't open file: " + path, e);
        }

        try {
            keys = FileChannel.open(keysPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long keysSize = keys.size();
            if (keysSize >= Integer.MAX_VALUE) {
                throw new IllegalStateException("keys too big at " + keysPath + ": " + keysSize);
            }
            keysPos = new AtomicInteger((int) keysSize);
        } catch (IOException e) {
            throw new UncheckedIOException("can't open file: " + path, e);
        }

        blobs = new Blobs(blobsPath, false);

        try {
            init();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to initialize data at " + path, e);
        }

        isClosed = new AtomicBoolean(false);
    }

    public long append(byte[] blob) {
        return blobs.append(blob);
    }

    /**
     * Return the value associated with the given key
     *
     * @param key the key to look up
     * @return the value associated with the key, or {@code Long.MIN_VALUE} if
     *         the key was not found
     */
    public long get(LookupKey key) {
        synchronized (memMonitor) {
            return mem.getLong(key);
        }
    }

    public boolean isDirty() {
        return isDirty.get();
    }

    public Blobs getBlobs() {
        return blobs;
    }

    /**
     * number of keys in this Lookup Data
     * @return the number of keys
     */
    public long size() {
        try {
            return chan.size() / 16;
        } catch (IOException e) {
            // This means the object has been close and its size for most purposes is now 0
            log.trace("Could not get channel size", e);
            return 0;
        }
    }

    /**
     * Set the value associated with the given key and return the prior value
     *
     * @param key the key whose value to set
     * @param value the new value set
     * @return the old value associated with the key, or {@code Long.MIN_VALUE}
     *         if the entry didn't exist yet
     */
    public long put(LookupKey key, final long value) {
        log.trace("putting {}={} in {}", key, value, path);

        final long existingValue;
        final int index;
        final boolean existing;

        synchronized (memMonitor) {
            existingValue = mem.put(key, value);
            if (existing = existingValue != Long.MIN_VALUE) {
                index = memOrder.getInt(key);
                if (index == Integer.MIN_VALUE) {
                    throw new IllegalStateException("unknown index order for existing key: " + key);
                }
            } else {
                index = memOrder.size();
                if (memOrder.put(key, index) != Integer.MIN_VALUE) {
                    throw new IllegalStateException("encountered repeated mem order key at index " + index + ": " + key);
                }
            }
        }

        if (existing) {
            set(index, value);
        } else {
            set(index, key, value);
        }

        return existingValue;
    }

    public long putIfNotExists(LookupKey key, final long value) {
        log.trace("putting (if not exists) {}={} in {}", key, value, path);

        final int index;

        synchronized (memMonitor) {
            long existingValue = mem.getLong(key);
            if (existingValue != Long.MIN_VALUE) {
                return existingValue;
            }
            if (mem.put(key, value) != Long.MIN_VALUE) {
                throw new IllegalStateException("race while performing conditional put within synchronized block");
            }

            index = memOrder.size();
            if (memOrder.put(key, index) != Integer.MIN_VALUE) {
                throw new IllegalStateException("encountered repeated mem order key at index " + index + ": " + key);
            }
        }
        set(index, key, value);
        return value;
    }

    public long putIfNotExists(LookupKey key, LongSupplier allocateLongFunc) {
        log.trace("putting (if not exists) {}=<lambda> in {}", key, path);

        final long newValue;
        final int index;

        synchronized (memMonitor) {
            long firstExistingValue = mem.getLong(key);
            if (firstExistingValue != Long.MIN_VALUE) {
                return firstExistingValue;
            }

            newValue = allocateLongFunc.getAsLong();
            long existingValue = mem.put(key, newValue);

            if (existingValue != Long.MIN_VALUE) {
                throw new IllegalStateException("race while putting (if not exists) " + key + "=<lambda> in " + path);
            }
            index = memOrder.size();
            if (memOrder.put(key, index) != Integer.MIN_VALUE) {
                throw new IllegalStateException("encountered repeated mem order key at index " + index + ": " + key);
            }
        }

        set(index, key, newValue);

        return newValue;
    }

    public long increment(LookupKey key, long delta) {
        log.trace("incrementing {} by {} in {}", key, delta, path);

        final long existingValue;
        final long newValue;
        final int index;
        final boolean existing;

        synchronized (memMonitor) {
            existingValue = mem.getLong(key);
            if (existing = existingValue != Long.MIN_VALUE) {
                newValue = existingValue + delta;
                index = memOrder.getInt(key);
                if (index == Integer.MIN_VALUE) {
                    throw new IllegalStateException("unknown index order for existing key: " + key);
                }
            } else {
                newValue = delta;
                index = memOrder.size();
                if (memOrder.put(key, index) != Integer.MIN_VALUE) {
                    throw new IllegalStateException("encountered repeated mem order key at index " + index + ": " + key);
                }
            }
            if (mem.put(key, newValue) != existingValue) {
                throw new IllegalStateException("race while incrementing key " + key + " in " + path);
            }
        }

        if (existing) {
            set(index, newValue);
        } else {
            set(index, key, newValue);
        }

        return newValue;
    }

    private void set(int index, LookupKey key, long value) {
        try {
            long keyPosAndSize = appendKey(key);
            ByteBuffer buf = recordBufSupplier.get();
            buf.putLong(keyPosAndSize);
            buf.putLong(value);
            buf.flip();
            chan.write(buf, index * 16);
            isDirty.set(true);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write key: " + key, e);
        }
    }

    private long appendKey(LookupKey key) throws IOException {
        byte[] keyBytes = key.bytes();
        int pos = keysPos.getAndAdd(keyBytes.length);
        keys.write(ByteBuffer.wrap(keyBytes), pos);
        return (long) pos << 32 | keyBytes.length & 0xffffffffL;
    }

    private void set(int index, long value) {
        try {
            ByteBuffer buf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
            buf.putLong(value);
            buf.flip();
            chan.write(buf, index * 16 + 8);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write value (" + value + ") at index: " + index, e);
        }
    }

    @Override
    public void close() throws IOException {
        log.trace("closing lookup data at {}", path);
        synchronized (chan) {
            if (isClosed.compareAndSet(false, true)) {

                try {
                    blobs.close();
                } catch (Exception e){
                    log.error("unable to close blobs", e);
                }

                try {
                    keys.close();
                } catch (Exception e) {
                    log.error("unable to close keys", e);
                }
                chan.close();
                log.trace("closed lookup data at {}", path);
                LookupMetadata metadata = generateMetadata();
                metadata.writeTo(metadataPath);
                log.trace("wrote lookup metadata at {}: {}", metadataPath, metadata);
            } else {
                log.warn("lookup data already closed: " + path, new RuntimeException("already closed") /* get stack */);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (!isDirty.getAndSet(false)) return;

        synchronized (chan) {
            if (isClosed.get()) {
                log.debug("ignoring flush of closed lookup data at {}", path);
                return;
            }
            log.trace("flushing lookup and metadata at {}", metadataPath);
            blobs.flush();
            keys.force(true);
            chan.force(true);
            LookupMetadata metadata = generateMetadata();
            metadata.writeTo(metadataPath);
            log.trace("flushed lookup and metadata at {}: {}", metadataPath, metadata);
        }
    }

    private void init() throws IOException {
        synchronized (memMonitor) {
            mem = new Object2LongAVLTreeMap<>();
            mem.defaultReturnValue(Long.MIN_VALUE);

            memOrder = new Object2IntLinkedOpenHashMap<>();
            memOrder.defaultReturnValue(Integer.MIN_VALUE);

            long size = chan.size();
            int numEntries = (int) (size / 16);
            long expectedSize = numEntries * 16;
            if (size != expectedSize) {
                log.error("truncating at pos " + expectedSize + " for file of corrupted size " + size);
                chan.truncate(expectedSize);
            }

            byte[] allKeysBytes = Files.readAllBytes(keysPath);
            byte[] allChanBytes = Files.readAllBytes(path);

            LongBuffer lbuf = ByteBuffer.wrap(allChanBytes).asLongBuffer();

            for (int i = 0; i < numEntries; i++) {
                int keyPosAndSizeIndex = i * 2;
                int valIndex = keyPosAndSizeIndex + 1;
                long keyPosAndSize = lbuf.get(keyPosAndSizeIndex);
                int keyPos = (int) (keyPosAndSize >> 32);
                int keySize = (int) keyPosAndSize;
                byte[] keyBytes = new byte[keySize];
                System.arraycopy(allKeysBytes, keyPos, keyBytes, 0, keySize);
                LookupKey key = new LookupKey(keyBytes);
                long val = lbuf.get(valIndex);
                if (mem.put(key, val) != Long.MIN_VALUE) {
                    throw new IllegalStateException("encountered repeated mem key at entry index " + i + ": " + key);
                }
                if (memOrder.put(key, memOrder.size()) != Integer.MIN_VALUE) {
                    throw new IllegalStateException("encountered repeated mem order key at entry index " + i + ": " + key);
                }
            }

            chan.position(expectedSize);

            if (chan.position() != chan.size()) {
                log.warn("init incomplete at pos " + chan.position() + " / " + chan.size());
            }
        }
    }

    private LookupMetadata generateMetadata() {
        final LookupKey minKey;
        final LookupKey maxKey;
        final int[] keyStorageOrder;
        final String[] keyStrings;

        synchronized (memMonitor) {
            minKey = mem.firstKey();
            maxKey = mem.lastKey();
            keyStorageOrder = memOrder.values().toIntArray();
            keyStrings = memOrder.keySet().stream().map(LookupKey::toString).toArray(String[]::new);
        }

        IntArrayCustomSort.sort(keyStorageOrder, (a, b) -> keyStrings[a].compareTo(keyStrings[b]));
        return new LookupMetadata(
                keyStrings.length,
                minKey,
                maxKey,
                keyStorageOrder
        );
    }
    
    static Stream<LookupKey> keys(Path path) {
        KeyIterator iter;
        try {
            iter = new KeyIterator(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to create key iterator for path: " + path, e);
        }
        Spliterator<LookupKey> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true).onClose(iter::close);
    }

    static Stream<Map.Entry<String, Long>> stream(Path path) {
        if (Files.notExists(path)) {
            return Stream.empty();
        }

        long size;
        try {
            size = Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to get size " + path, e);
        }

        Path keysPath = path.resolveSibling("keys");

        int numEntries = (int) (size / 16);
        long expectedSize = numEntries * 16;
        if (size != expectedSize) {
            log.warn("unexpected size for " + path + ": expected " + expectedSize + ", got " + size);
        }

        byte[] allKeysBytes;
        try {
            allKeysBytes = Files.readAllBytes(keysPath);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read keys fully at " + keysPath, e);
        }
        byte[] allChanBytes;
        try {
            allChanBytes = Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read fully at " + path, e);
        }

        LongBuffer lbuf = ByteBuffer.wrap(allChanBytes).asLongBuffer();

        return IntStream.range(0, numEntries).parallel().mapToObj(i -> {
            int keyPosAndSizeIndex = i * 2;
            int valIndex = keyPosAndSizeIndex + 1;
            long keyPosAndSize = lbuf.get(keyPosAndSizeIndex);
            int keyPos = (int) (keyPosAndSize >> 32);
            int keySize = (int) keyPosAndSize;
            byte[] keyBytes = new byte[keySize];
            System.arraycopy(allKeysBytes, keyPos, keyBytes, 0, keySize);
            LookupKey key = new LookupKey(keyBytes);
            long val = lbuf.get(valIndex);

            return Maps.immutableEntry(key.string(), val);
        });

    }

    static void scan(Path path, BiConsumer<String, Long> keyValueFunction) {
        if (Files.notExists(path)) {
            return;
        }

        long size;
        try {
            size = Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to get size " + path, e);
        }

        Path keysPath = path.resolveSibling("keys");

        int numEntries = (int) (size / 16);
        long expectedSize = numEntries * 16;
        if (size != expectedSize) {
            log.warn("unexpected size for " + path + ": expected " + expectedSize + ", got " + size);
        }

        byte[] allKeysBytes;
        try {
            allKeysBytes = Files.readAllBytes(keysPath);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read keys fully at " + keysPath, e);
        }
        byte[] allChanBytes;
        try {
            allChanBytes = Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read fully at " + path, e);
        }

        LongBuffer lbuf = ByteBuffer.wrap(allChanBytes).asLongBuffer();

        IntStream.range(0, numEntries).parallel().forEach(i -> {
            int keyPosAndSizeIndex = i * 2;
            int valIndex = keyPosAndSizeIndex + 1;
            long keyPosAndSize = lbuf.get(keyPosAndSizeIndex);
            int keyPos = (int) (keyPosAndSize >> 32);
            int keySize = (int) keyPosAndSize;
            byte[] keyBytes = new byte[keySize];
            System.arraycopy(allKeysBytes, keyPos, keyBytes, 0, keySize);
            LookupKey key = new LookupKey(keyBytes);
            long val = lbuf.get(valIndex);
            keyValueFunction.accept(key.string(), val);
        });
    }



    private static class KeyIterator implements Iterator<LookupKey>, AutoCloseable {
        private final Path path;
        private final Path keysPath;
        private final FileChannel chan;
        private final FileChannel keysChan;
        private final int numKeys;
        private int keyIndex = 0;

        KeyIterator(Path path) throws IOException {
            this.path = path;
            chan = FileChannel.open(path, StandardOpenOption.READ);
            numKeys = (int) (chan.size() / 16);
            keysPath = path.resolveSibling("keys");
            keysChan = numKeys > 0 ? FileChannel.open(keysPath, StandardOpenOption.READ) : null;
        }

        int getNumKeys() {
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
                key = LookupData.readKey(chan, keysChan, keyIndex);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to read at key index " + keyIndex + " from " + path, e);
            }
            keyIndex++;
            return key;
        }

        @Override
        public void close() {
            try {
                chan.close();
            } catch (IOException e) {
                log.error("trouble closing: " + path, e);
            }
            try {
                keysChan.close();
            } catch (IOException e) {
                log.error("trouble closing: " + keysPath, e);
            }
        }
    }


    static Long size(Path path) {
        if (Files.notExists(path)) {
            return 0L;
        }
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
            return chan.size() / 16;
        } catch (IOException e) {
            throw new UncheckedIOException("unable to get size of " + path, e);
        }
    }


    static int numEntries(FileChannel chan) throws IOException {
        return (int) (chan.size() / 16);
    }

    static LookupKey readKey(FileChannel chan, FileChannel keysChan, int keyNumber) throws IOException {
        ByteBuffer longBuf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
        long pos = keyNumber * 16;
        chan.read(longBuf, pos);
        longBuf.flip();
        long keyPosAndSize = longBuf.getLong();
        int keyPos = (int) (keyPosAndSize >> 32);
        int keySize = (int) keyPosAndSize;
        byte[] keyBytes = new byte[keySize];
        keysChan.read(ByteBuffer.wrap(keyBytes), keyPos);
        return new LookupKey(keyBytes);
    }

    static long readValue(FileChannel chan, int keyIndex) throws IOException {
        long pos = keyIndex * 16 + 8;
        ByteBuffer buf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
        chan.read(buf, pos);
        buf.flip();
        return buf.getLong();
    }
}
