package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
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
    private final BlockedLongPairs keyPosToBlockPos;

    // Flushing every 30 seconds, we can run for 2000 years before the metaDataGeneration hits INTEGER.MAX_VALUE
    private AtomicInteger metaDataGeneration;


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

        keyPosToBlockPos = new BlockedLongPairs(dataPath, partitionLookupCache.getPageCache());

        metaDataGeneration = new AtomicInteger();

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
     * Set the value of the key if it does not exist or return the existing value
     *
     * @param key the key to check or set
     * @param allocateLongFunc the function to call to get the value if this is a new key
     * @return the value associated with the key
     */
    public long putIfNotExists(LookupKey key, LongSupplier allocateLongFunc) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        AtomicReference<Long> ref = new AtomicReference<>();
        writeCache.computeIfAbsent(key, k -> {
            Long existingValue = getCached(k);
            if (existingValue != null) {
                ref.set(existingValue);
                return null;
            } else {
                k.setDirty(true);
                long val = allocateLongFunc.getAsLong();
                ref.set(val);
                return val;
            }
        });

        return ref.get();
    }

    private Long getCached(LookupKey key) {
        return partitionLookupCache.getLong(key, this::findValueFor);
    }

    /**
     * Load the key and cache it for future use
     * @param keyNumber the index into the block of long pairs that map key blob position to key's value
     * @return the cached lookup key
     */
    public LookupKey getKey(int keyNumber) {
        long keyPos = keyPosToBlockPos.getLeft(keyNumber);
        long value = keyPosToBlockPos.getRight(keyNumber);

        LookupKey key = new LookupKey(keyBlobs.read(keyPos));
        partitionLookupCache.putLookup(key, value);

        return key;
    }

    /**
     * Read the long value associated with a particular key number
     * @param keyNumber the index into the block of long pairs that map key blob position to key's value
     * @return the long value
     */
    public long getKeyValue(int keyNumber){
        return keyPosToBlockPos.getRight(keyNumber);
    }

    /**
     * load a key from paged files for the partition lookup cache
     * Must return null to prevent loading missing value into cache
     *
     * @param key the Key we are looking for
     * @return Long value or null if not present
     */
    private Long findValueFor(PartitionLookupKey key) {
        return findValueFor(key.getLookupKey());
    }

    /**
     * Load a key from cached pages
     *
     * @param key the key we are looking for
     * @return Long value or null if not present
     */
    private Long findValueFor(LookupKey key) {
        LookupMetadata metadata = partitionLookupCache.getMetadata(this);
        return metadata.readData(this, key);
    }

    /**
     * The path for the metadata
     * @return the path
     */
    public Path getMetadataPath(){
        return metadataPath;
    }

    public Path getHashPath(){
        return hashPath;
    }

    public int getMetaDataGeneration(){
        return metaDataGeneration.get();
    }

    /**
     * Check if this LookupData is dirty before calling flush
     *
     * @return
     */
    public boolean isDirty() {
        return isDirty.get();
    }

    @Override
    public void close() throws IOException {
        log.trace("closing lookup data at {}", hashPath);
        if (isClosed.compareAndSet(false, true)) {

            flush();
            keyBlobs.close();
            keyPosToBlockPos.close();

        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (readOnly) throw new RuntimeException("Can not flush read only LookupData: " + hashPath);

        if (writeCache.size() > 0) {

            Set<LookupKey> keys = new HashSet<>(writeCache.keySet());

            // Check the metadata generation of the LookupKeys - update if they are old - Optimistic...lookup?

            LookupMetadata currentMetadata = partitionLookupCache.getMetadata(this);

            // Now stream the keys and do sorted merge join on the keyStorageOrder from the current metadata

            keys.stream()
                    .parallel()
                    .forEach(key ->
                            writeCache.computeIfPresent(key, (k, value) -> {
                                long pos = keyBlobs.append(k.bytes());
                                keyPosToBlockPos.append(pos, value);

                                k.setDirty(false);
                                return value;
                            })
                    );

            // Any write Cache entries not in keys with the current generation will need to be updated

            partitionLookupCache.putMetadata(this, generateMetadata());

            keys.stream()
                    .parallel()
                    .forEach(key ->
                        writeCache.computeIfPresent(key, (k, value) ->{
                            // Remove all the values that are still clean
                            if (key.isDirty()){
                                return value;
                            } else {
                                return null;
                            }
                        })

                    );
        }
    }


    private LookupMetadata generateMetadata() {
        final LookupKey minKey;
        final LookupKey maxKey;
        final int[] keyStorageOrder = new int[]{};


        //IntArrayCustomSort.sort(keyStorageOrder, (a, b) -> keyStrings[a].compareTo(keyStrings[b]));
        return new LookupMetadata(
                keyStorageOrder.length,
                getKey(keyStorageOrder[0]),
                getKey(keyStorageOrder[keyStorageOrder.length - 1]),
                keyStorageOrder,
                metaDataGeneration.incrementAndGet()
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
}
