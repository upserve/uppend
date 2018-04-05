package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.*;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

public class LookupData implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final PartitionLookupCache partitionLookupCache;

    // The container for stuff we need to write
    private final ConcurrentHashMap<LookupKey, Long> writeCache;

    private final AtomicBoolean isClosed;
    private final AtomicBoolean isDirty;

    private final Path hashPath;
    private final Path dataPath;
    private final Path metadataPath;
    private final Path keyPath;

    private final Supplier<ByteBuffer> recordBufSupplier;

    private final boolean readOnly;

    private final Blobs keyBlobs;

    public LookupData(Path path, PartitionLookupCache lookupCache) {
        log.trace("opening hashpath for lookup: {}", path);

        this.hashPath = path;
        this.dataPath = path.resolve("data");
        this.metadataPath = path.resolve("meta");
        this.keyPath = path.resolve("keys");

        this.partitionLookupCache = lookupCache;


        if (lookupCache.getPageCache().getFileCache().readOnly()) {
            readOnly = true;
            writeCache = null;
        } else {
            readOnly = false;
            writeCache = new ConcurrentHashMap<>();
        }

        if (!Files.exists(path) /* notExists() returns true erroneously */) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to make parent dir: " + path, e);
            }
        }

        recordBufSupplier = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(16);

        keyBlobs = new Blobs(keyPath, partitionLookupCache.getPageCache());

        try {
            init();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to initialize lookup data at " + hashPath, e);
        }

        isClosed = new AtomicBoolean(false);
        isDirty = new AtomicBoolean(false);
    }

    /**
     * Return the value associated with the given key
     *
     * @param key the key to look up
     * @return the value associated with the key, null if the key was not found
     */
    public Long get(LookupKey key) {
        if (readOnly) {
            return getCached(key);
        } else {
            return writeCache.getOrDefault(key, getCached(key));
        }
    }

    /**
     * Set the value associated with the given key and return the prior value
     *
     * @param key the key whose value to set
     * @param value the new value set
     * @return the old value associated with the key, or null if the entry didn't exist yet
     */
    public Long put(LookupKey key, final long value) {
        if (readOnly) throw new RuntimeException("Can not put in read only LookupData: " + hashPath);

        AtomicReference<Long>  ref = new AtomicReference<>();
        writeCache.compute(key, (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = getCached(k);
            }

            if (value != oldValue) k.setDirty(true);
            // This will add keys which are not dirty to the write cache but the write cache will no-op on flush
            ref.set(oldValue);
            return value;
        });
        return ref.get();
    }

    /**
     * Set the value of the key if it does not exist or return the existing value
     *
     * @param key the key to check or set
     * @param allocateLongFunc the function to call to get the value if this is a new key
     * @return the value associated with the key
     */
    public long putIfNotExists(LookupKey key, LongSupplier allocateLongFunc) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        return writeCache.compute(key, (k, value) -> {
            if (value == null) {
                // If not write cached, check the read cache
                value = getCached(k);
                if (value == null) {
                    // if not read cached call the long supplier and mark it dirty in the write cache
                    value = allocateLongFunc.getAsLong();
                    k.setDirty(true);
                }
                // This will add values from the read cache to the write cache but they will be clean and no-op on flush
            }
            return value;
        });
    }

    /**
     * Increment the value of the key if it does not exist or return the existing value
     *
     * @param key the key to check or set
     * @param delta the amount to increment
     * @return the new value associated with the key
     */
    public long increment(LookupKey key, long delta) {
        if (readOnly) throw new RuntimeException("Can not increment read only LookupData: " + hashPath);

        return writeCache.compute(key, (k, value) -> {
            k.setDirty(true);
            if (value == null) {
                return getCached(k, 0L) + delta;
            } else{
                return value + delta;
            }
        });
    }

    private long getCached(LookupKey key, long defaultValue){
        Long result = getCached(key);
        if (result == null) {
            return defaultValue;
        } else {
            return result;
        }
    }

    private Long getCached(LookupKey key){
        return partitionLookupCache.getLong(key, this::loadKey);
    }


    /**
     * load a key from paged files for the partition lookup cache
     * Must return null to prevent loading missing value into cache
     *
     * @param key the Key we are looking for
     * @return OptionalLong value
     */
    private Long loadKey(PartitionLookupKey key) {
        return loadKey(key.getLookupKey());
    }

    /**
     * Load a key from cached pages
     *
     * @param key the key we are looking for
     * @return OptionalLong value
     */

    private Long loadKey(LookupKey key) {
        return 0L;
    }






    /**
     * Check if this LookupData is dirty before calling flush
     *
     * @return
     */
    public boolean isDirty() {
        return isDirty.get();
    }

    private void set(int index, LookupKey key, long value) {
        isDirty.set(true);
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
        isDirty.set(true);
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
        log.trace("closing lookup data at {}", hashPath);
        synchronized (chan) {
            if (isClosed.compareAndSet(false, true)) {
                try {
                    keyBlobs.close();
                } catch (Exception e) {
                    log.error("unable to close key blobs", e);
                }
                chan.close();
                log.trace("closed lookup data at {}", hashPath);
                LookupMetadata metadata = generateMetadata();
                metadata.writeTo(metadataPath);
                log.trace("wrote lookup metadata at {}: {}", metadataPath, metadata);
            } else {
                log.warn("lookup data already closed: " + dataPath, new RuntimeException("already closed") /* get stack */);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (readOnly)  throw new RuntimeException("Can not flush read only LookupData: " + hashPath);

        if (writeCache.size() > 0){

            writeCache
                    .keySet()
                    .stream()
                    .filter(LookupKey::isDirty)
                    .forEach(key -> {
                        writeCache.computeIfPresent(key, (k, value)-> {

                            

                            return value;
                        });

                    });


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
            throw new UncheckedIOException("unable to create key iterator for dataPath: " + path, e);
        }
        Spliterator<LookupKey> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true).onClose(iter::close);
    }

    static Stream<Map.Entry<String, Long>> scan(Path path) {
        if (Files.notExists(path)) {
            return Stream.empty();
        }
        KeyLongIterator iter;
        try {
            iter = new KeyLongIterator(path);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to create key iterator for dataPath: " + path, e);
        }
        Spliterator<Map.Entry<String, Long>> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true).onClose(iter::close);
    }

    static void scan(Path path, ObjLongConsumer<String> keyValueFunction) {
        if (Files.notExists(path)) {
            return;
        }
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
            try (Blobs keyBlobs = new Blobs(path.resolveSibling("keys"), new PagedFileMapper(32))) {
                chan.position(0);
                long pos = 0;
                long size = chan.size();
                DataInputStream dis = new DataInputStream(new BufferedInputStream(Channels.newInputStream(chan), 8192));
                while (pos < size) {
                    long nextPos = pos + 16;
                    if (nextPos > size) {
                        log.warn("scanned past size (" + size + ") of file (" + path + ") at pos " + pos);
                        break;
                    }
                    long keyPos = dis.readLong();
                    byte[] keyBytes = keyBlobs.read(keyPos);
                    LookupKey key = new LookupKey(keyBytes);
                    long val = dis.readLong();
                    keyValueFunction.accept(key.string(), val);
                    pos = nextPos;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable to scan " + path, e);
        }
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

    private static class KeyLongIterator implements Iterator<Map.Entry<String, Long>>, AutoCloseable {
        private final Path path;
        private final Path keysPath;
        private final FileChannel chan;

        private final Blobs keyBlobs;
        private final int numKeys;
        private int keyIndex = 0;
        private final DataInputStream dis;

        KeyLongIterator(Path path) throws IOException {
            this.path = path;
            chan = FileChannel.open(path, StandardOpenOption.READ);
            chan.position(0);
            keysPath = path.resolveSibling("keys");
            keyBlobs = new Blobs(keysPath, new PagedFileMapper(32));
            numKeys = (int) chan.size() / 16;
            dis = new DataInputStream(new BufferedInputStream(Channels.newInputStream(chan), 8192));
        }

        int getNumKeys() {
            return numKeys;
        }

        @Override
        public boolean hasNext() {
            return keyIndex < numKeys;
        }

        @Override
        public Map.Entry<String, Long> next() {
            try {
                long keyPos = dis.readLong();
                byte[] keyBytes = keyBlobs.read(keyPos);
                LookupKey key = new LookupKey(keyBytes);
                long val = dis.readLong();
                keyIndex++;
                return Maps.immutableEntry(key.string(), val);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to read at key index " + keyIndex + " from " + path, e);
            }
        }

        @Override
        public void close() {
            try {
                chan.close();
            } catch (IOException e) {
                log.error("trouble closing: " + path, e);
            }
            keyBlobs.close();
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
