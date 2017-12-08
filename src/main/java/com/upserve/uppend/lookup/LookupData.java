package com.upserve.uppend.lookup;

import com.upserve.uppend.Blobs;
import com.upserve.uppend.util.*;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
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
    private final Supplier<ByteBuffer> recordBufSupplier;
    private final FileChannel chan;

    private final Object memMonitor = new Object();
    private Object2LongSortedMap<LookupKey> mem;
    private Object2IntLinkedOpenHashMap<LookupKey> memOrder;

    private final Blobs keyBlobs;

    public LookupData(Path path, Path metadataPath) {
        log.trace("opening lookup data: {}", path);

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

        recordBufSupplier = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(16);

        try {
            chan = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException("can't open file: " + path, e);
        }

        keyBlobs = new Blobs(path.resolveSibling("keys"));

        try {
            init();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to initialize data at " + path, e);
        }

        isClosed = new AtomicBoolean(false);
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
            long existingValue = mem.put(key, value);
            if (existingValue != Long.MIN_VALUE) {
                return existingValue;
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
            long keyPos = keyBlobs.append(key.bytes());
            ByteBuffer buf = recordBufSupplier.get();
            buf.putLong(keyPos);
            buf.putLong(value);
            buf.flip();
            chan.write(buf, index * 16);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write key: " + key, e);
        }
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
                    keyBlobs.close();
                } catch (Exception e) {
                    log.error("unable to close key blobs", e);
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
        synchronized (chan) {
            if (isClosed.get()) {
                log.debug("ignoring flush of closed lookup data at {}", path);
                return;
            }
            log.trace("flushing lookup and metadata at {}", metadataPath);
            keyBlobs.flush();
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

            chan.position(0);
            long pos = 0;
            long size = chan.size();
            DataInputStream dis = new DataInputStream(new BufferedInputStream(Channels.newInputStream(chan), 8192));
            // don't call dis.close() on wrapping InputStream since we don't want it to chain to chan.close()
            while (pos < size) {
                long nextPos = pos + 16;
                if (nextPos > size) {
                    // corrupt; fix
                    log.error("truncating at pos " + pos + " for file of corrupted size " + size);
                    chan.truncate(pos);
                    break;
                }
                long keyPos;
                try {
                    keyPos = dis.readLong();
                } catch (IOException e) {
                    throw new IOException("read bad key pos at pos " + pos, e);
                }
                byte[] keyBytes = keyBlobs.read(keyPos);
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
        return StreamSupport.stream(spliter, true);
    }

    static int numEntries(FileChannel chan) throws IOException {
        return (int) (chan.size() / 16);
    }

    static LookupKey readKey(FileChannel chan, FileChannel keysChan, int keyNumber) throws IOException {
        ByteBuffer longBuf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
        long pos = keyNumber * 16;
        chan.read(longBuf, pos);
        longBuf.flip();
        long keyPos = longBuf.getLong();
        byte[] keyBytes = Blobs.read(keysChan, keyPos);
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
