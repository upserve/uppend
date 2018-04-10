package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;

public class LookupData implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final PartitionLookupCache partitionLookupCache;

    // The container for stuff we need to write
    private final ConcurrentHashMap<LookupKey, Long> writeCache; // Only new keys can be in the write cache

    private final Path hashPath;
    private final Path dataPath;
    private final Path metadataPath;
    private final Path keyPath;

    private final boolean readOnly;

    private final Blobs keyBlobs;
    private final BlockedLongPairs keyPosToBlockPos;

    private final ReadWriteLock flushLock;
    private final Lock readLock;
    private final Lock writeLock;

    // Flushing every 30 seconds, we can run for 2000 years before the metaDataGeneration hits INTEGER.MAX_VALUE
    private AtomicInteger metaDataGeneration;

    /**
     *     Use this file to decide if the LookupData is present for a hashPath
     */
    public static Path metadataPath(Path hashPath){
        return hashPath.resolve("meta");
    }

    public LookupData(Path path, PartitionLookupCache lookupCache) {
        log.trace("opening hashpath for lookup: {}", path);

        this.hashPath = path;
        this.dataPath = path.resolve("data");
        this.metadataPath = metadataPath(path);
        this.keyPath = path.resolve("keys");

        this.partitionLookupCache = lookupCache;


        if (lookupCache.getPageCache().getFileCache().readOnly()) {
            readOnly = true;
            writeCache = null;
        } else {
            readOnly = false;
            writeCache = new ConcurrentHashMap<>();
        }

        if (!Files.exists(path) || !Files.isDirectory(path) /* notExists() returns true erroneously */) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to make parent dir: " + path, e);
            }
        }

        keyBlobs = new Blobs(keyPath, partitionLookupCache.getPageCache());

        keyPosToBlockPos = new BlockedLongPairs(dataPath, partitionLookupCache.getPageCache());

        metaDataGeneration = new AtomicInteger();

        flushLock = new ReentrantReadWriteLock();
        readLock = flushLock.readLock();
        writeLock = flushLock.writeLock();
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
     * Set the value of the key if it does not exist and return the new value. If it does exist, return the existing value.
     *
     * @param key the key to check or set
     * @param allocateLongFunc the function to call to getLookupData the value if this is a new key
     * @return the value associated with the key
     */
    public long putIfNotExists(LookupKey key, LongSupplier allocateLongFunc) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        AtomicReference<Long> ref = new AtomicReference<>();
        writeCache.compute(key, (k, value) -> {
            if (value == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    long val = allocateLongFunc.getAsLong();
                    ref.set(val);
                    return val;

                } else {
                    ref.set(existingValue);
                    return null;
                }
            } else {
                ref.set(value);
                return value;
            }
        });

        return ref.get();
    }

    /**
     * Set the value of the key if it does not exist and return the new value. If it does exist, return the existing value.
     *
     * @param key the key to check or set
     * @param value the value to put if this is a new key
     * @return the value associated with the key
     */
    public long putIfNotExists(LookupKey key, long value) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        AtomicReference<Long> ref = new AtomicReference<>();
        writeCache.compute(key, (k, val) -> {
            if (val == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref.set(value);
                    return value;

                } else {
                    ref.set(existingValue);
                    return null;
                }
            } else {
                ref.set(val);
                return val;
            }
        });

        return ref.get();
    }

    /**
     * Increment the value associated with this key by the given amount
     *
     * @param key the key to be incremented
     * @param delta the amount to increment the value by
     * @return the value new associated with the key
     */
    public long increment(LookupKey key, long delta) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        AtomicReference<Long> ref = new AtomicReference<>();
        writeCache.compute(key, (k, value) -> {
            if (value == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref.set(delta);
                    return delta; // must write a new key with delta as the value when we flush

                } else {
                    existingValue += delta;
                    ref.set(existingValue);

                    partitionLookupCache.putLookup(key, existingValue); // Update the read cache
                    keyPosToBlockPos.writeRight(key.getLookupBlockIndex(), existingValue); // Update the value on disk

                    // No need to add this to the write cache
                    return null;
                }
            } else {
                // This value is only in the write cache!
                value += delta;
                ref.set(value);
                return value;
            }
        });

        return ref.get();
    }


    /**
     * Set the value of this key.
     *
     * @param key the key to check or set
     * @param value the value to set for this key
     * @return the previous value associated with the key or null if it did not exist
     */
    public Long put(LookupKey key, final long value) {
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData: " + hashPath);

        AtomicReference<Long> ref = new AtomicReference<>(); // faster to replace with a Long[] ?
        writeCache.compute(key, (k, val) -> {
            if (val == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref.set(null);
                    return value; // must write a new key with delta as the value when we flush

                } else {
                    ref.set(existingValue);

                    partitionLookupCache.putLookup(key, value); // Update the read cache
                    keyPosToBlockPos.writeRight(key.getLookupBlockIndex(), value); // Update the value on disk

                    // No need to add this to the write cache
                    return null;
                }
            } else {
                // This value is only in the write cache!
                ref.set(val);
                return value;
            }
        });

        return ref.get();
    }


    private Long getCached(LookupKey key) {
        return partitionLookupCache.getLong(key, this::findValueFor);
    }

    /**
     * read the LookupKey by index
     * @param keyNumber the index into the block of long pairs that map key blob position to key's value
     * @return the cached lookup key
     */
    public LookupKey readKey(int keyNumber) {
        long keyPos = keyPosToBlockPos.getLeft(keyNumber);

        LookupKey key = new LookupKey(keyBlobs.read(keyPos));
        key.setLookupBlockIndex(keyNumber);

        return key;
    }

    /**
     * Used in iterators return an entry containing the key as a string and the value
     * @param keyNumber the index into the long data
     * @return
     */
    public Map.Entry<String, Long> readEntry(int keyNumber) {
        long keyPos = keyPosToBlockPos.getLeft(keyNumber);
        long value = keyPosToBlockPos.getRight(keyNumber);

        LookupKey key = new LookupKey(keyBlobs.read(keyPos));
        key.setLookupBlockIndex(keyNumber);

        return Maps.immutableEntry(key.string(), value);
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
        return partitionLookupCache.getMetadata(this).findLong(this, key);
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
     * Create a copy of the keys currently in the write cache
     * @return the key set
     */
    private Set<LookupKey> writeCacheKeySetCopy(){
        if (writeCache != null) {
            return new HashSet<>(writeCache.keySet());
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Create a copy of the write cache
     * @return the copy of the Map
     */
    private Map<LookupKey, Long> writeCacheCopy(){
        if (writeCache != null) {
            return new HashMap<>(writeCache);
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (readOnly) throw new RuntimeException("Can not flush read only LookupData: " + hashPath);

        if (writeCache.size() > 0) {

            Set<LookupKey> keys = writeCacheKeySetCopy();

            LookupMetadata currentMetadata = partitionLookupCache.getMetadata(this);

            // Now stream the keys and do sorted merge join on the keyStorageOrder from the current metadata

            int currentMetadataGeneration = currentMetadata.getMetadataGeneration();

            Map<Long, List<LookupKey>> newKeysGroupedBySortOrderIndex;
            try {
                writeLock.lock();
                newKeysGroupedBySortOrderIndex = keys.stream()
                        .parallel()
                        .peek(key -> {
                            // Check the metadata generation of the LookupKeys
                            if (key.getMetaDataGeneration() != currentMetadataGeneration) {
                                // Update the index of the key which this new value sorts after
                                currentMetadata.findLong(this, key);
                            }
                        })
                        .map(key -> {

                                    long insertSortedAfterKeyAtIndex = key.getLookupBlockIndex();

                                    writeCache.computeIfPresent(key, (k, value) -> {
                                        long pos = keyBlobs.append(k.bytes());
                                        key.setLookupBlockIndex((int) keyPosToBlockPos.append(pos, value));

                                        partitionLookupCache.putLookup(key, value); // put it in the read cache and remove from the write cache
                                        return null;
                                    });

                                    return Maps.immutableEntry(insertSortedAfterKeyAtIndex, key);
                                }

                        ).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

            } finally {
                writeLock.unlock();
            }

            int[] currentKeySortOrder = currentMetadata.getKeyStorageOrder();

            int[] newKeySortOrder = new int[currentKeySortOrder.length + keys.size()];


            int index = 0;

            LookupKey minKey = currentMetadata.getMinKey();
            LookupKey maxKey = currentMetadata.getMaxKey();

            List<LookupKey> newKeys = null;

            if(newKeysGroupedBySortOrderIndex.containsKey(-1L)){
                newKeys = newKeysGroupedBySortOrderIndex.get(-1L);
                newKeys.sort(LookupKey::compareTo);

                minKey = newKeys.get(0);

                for(LookupKey key: newKeys){
                    newKeySortOrder[index] = key.getLookupBlockIndex();
                    index++;
                }
            }

            if (currentKeySortOrder.length == 0){
                if (newKeys == null) throw new RuntimeException("newKeys should never be null if currentKeysSortOrder is empty");
                maxKey = newKeys.get(newKeys.size() - 1);
            } else {
                for (int keyIndex : currentKeySortOrder) {

                    newKeySortOrder[index] = keyIndex;
                    index++;

                    if (newKeysGroupedBySortOrderIndex.containsKey((long) keyIndex)) {
                        newKeys = newKeysGroupedBySortOrderIndex.get((long) keyIndex);
                        newKeys.sort(LookupKey::compareTo);

                        for (LookupKey key : newKeys) {
                            newKeySortOrder[index] = key.getLookupBlockIndex();

                            if (index == newKeySortOrder.length) {
                                maxKey = key;
                            }

                            index++;
                        }

                    }
                }
            }

            partitionLookupCache.putMetadata(this,
                    LookupMetadata.generateMetadata(minKey, maxKey, newKeySortOrder, metadataPath, metaDataGeneration.incrementAndGet())
            );
            keyBlobs.flush();
            keyPosToBlockPos.flush();
        }
    }

    Stream<LookupKey> keys() {
        KeyIterator iter;
        try {
            readLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
            iter = new KeyIterator(this);
        } finally {
            readLock.unlock();
        }
        Spliterator<LookupKey> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true);
    }

    Stream<Map.Entry<String, Long>> scan() {

        KeyLongIterator iter;
        try {
            readLock.lock(); // Read lock the WriteCache while initializing the KeyLongIterator
            iter = new KeyLongIterator(this);
        } finally {
            readLock.unlock();
        }

        Spliterator<Map.Entry<String, Long>> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true);
    }

    void scan(ObjLongConsumer<String> keyValueFunction) {
        final int numKeys;
        final Map<LookupKey, Long> writeCacheCopy;
        try{
            readLock.lock(); // Read lock the WriteCache while initializing the data to scan
            numKeys = keyPosToBlockPos.getMaxIndex();

            writeCacheCopy = writeCacheCopy();
        } finally {
            readLock.unlock();
        }

        writeCacheCopy
                .forEach((key, value) -> keyValueFunction.accept(key.string(), value));

        // Read but do not cache these keys - easy to add but is it helpful?
        IntStream
                .range(0, numKeys)
                .mapToObj(this::readEntry)
                .forEach(entry -> keyValueFunction
                        .accept(entry.getKey(), entry.getValue()));
    }

    private static class KeyIterator implements Iterator<LookupKey> {

        private int keyIndex = 0;
        private final LookupData lookupData;
        private final int maxKeyIndex;
        private final int numKeys;
        private final Iterator<LookupKey> writeCacheKeyIterator;

        /**
         * Should be constructed inside a ReadLock for the LookupData to ensure a consistent snapshot
         * @param lookupData the keys to iterate
         */
        KeyIterator(LookupData lookupData) {
            this.lookupData = lookupData;
            // Get a snapshot of the keys
            Set<LookupKey> writeCacheKeys = lookupData.writeCacheKeySetCopy();
            maxKeyIndex = lookupData.keyPosToBlockPos.getMaxIndex();
            numKeys = maxKeyIndex + writeCacheKeys.size();
            writeCacheKeyIterator = writeCacheKeys.iterator();
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

            if (keyIndex < maxKeyIndex){
                key = lookupData.readKey(keyIndex); // Read but do not cache these keys - easy to add but is it helpful?
            } else {
                key = writeCacheKeyIterator.next();
            }

            keyIndex++;
            return key;
        }
    }

    private static class KeyLongIterator implements Iterator<Map.Entry<String, Long>> {

        private int keyIndex = 0;
        private final LookupData lookupData;
        private final int maxKeyIndex;
        private final int numKeys;
        private final Iterator<Map.Entry<String, Long>> writeCacheKeyIterator;

        /**
         * Should be constructed inside a ReadLock for the LookupData to ensure a consistent snapshot
         * @param lookupData the keys to iterate
         */
        KeyLongIterator(LookupData lookupData) {
            this.lookupData = lookupData;
            // Get a snapshot of the keys
            Map<LookupKey, Long> writeCacheCopy = lookupData.writeCacheCopy();
            maxKeyIndex = lookupData.keyPosToBlockPos.getMaxIndex();
            numKeys = maxKeyIndex + writeCacheCopy.size();
            writeCacheKeyIterator = writeCacheCopy
                    .entrySet()
                    .stream()
                    .map(entry -> Maps.immutableEntry(entry.getKey().string(), entry.getValue()))
                    .iterator();
        }

        int getNumKeys() {
            return numKeys;
        }

        @Override
        public boolean hasNext() {
            return keyIndex < numKeys;
        }

        @Override
        public Map.Entry<String, Long>  next() {
            Map.Entry<String, Long> result;

            if (keyIndex < maxKeyIndex){
                result = lookupData.readEntry(keyIndex); // Read but do not cache these keys - easy to add but is it helpful?
            } else {
                result = writeCacheKeyIterator.next();
            }

            keyIndex++;
            return result;
        }
    }
}
