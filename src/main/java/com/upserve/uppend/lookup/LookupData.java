package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
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
    protected final ConcurrentHashMap<LookupKey, Long> writeCache; // Only new keys can be in the write cache

    private final Path hashPath;
    private final Path dataPath;
    private final Path metadataPath;
    private final Path keyPath;

    private final boolean readOnly;

    private final LongBlobStore keyLongBlobs;

    private final ReadWriteLock flushLock;
    private final Lock readLock;
    private final Lock writeLock;

    // Flushing every 30 seconds, we can run for 2000 years before the metaDataGeneration hits INTEGER.MAX_VALUE
    private AtomicInteger metaDataGeneration;

    /**
     * Use this path to decide if the LookupData is present for a hashPath
     * @param hashPath the hashpath to build a metadata path for.
     * @return the path for the metadata file
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

        keyLongBlobs = new LongBlobStore(keyPath, partitionLookupCache.getPageCache());

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

        long[] ref = new long[1];
        writeCache.compute(key, (k, value) -> {
            if (value == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    long val = allocateLongFunc.getAsLong();
                    ref[0] = val;
                    return val;

                } else {
                    ref[0] = existingValue;
                    return null;
                }
            } else {
                ref[0] = value;
                return value;
            }
        });

        return ref[0];
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

        long[] ref = new long[1];
        writeCache.compute(key, (k, val) -> {
            if (val == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref[0] = value;
                    return value;

                } else {
                    ref[0] = existingValue;
                    return null;
                }
            } else {
                ref[0] = val;
                return val;
            }
        });

        return ref[0];
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

        long[] ref = new long[1];
        writeCache.compute(key, (k, value) -> {
            if (value == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref[0] = delta;
                    return delta; // must write a new key with delta as the value when we flush

                } else {
                    long newValue = existingValue + delta;
                    ref[0] = newValue;

                    partitionLookupCache.putLookup(key, newValue); // Update the read cache
                    // This is gross - find a way to hold onto the position???
                    LookupMetadata lookupMetadata = partitionLookupCache.getMetadata(this);
                    Long pos = lookupMetadata.findKeyPosition(this, key);
                    keyLongBlobs.writeLong(pos, newValue); // Update the value on disk

                    // No need to add this to the write cache
                    return null;
                }
            } else {
                // This value is only in the write cache!
                value += delta;
                ref[0] = value;
                return value;
            }
        });

        return ref[0];
    }

    /**
     * Set the value of this key.
     *
     * @param key the key to check or set
     * @param value the value to set for this key
     * @return the previous value associated with the key or null if it did not exist
     */
    public Long put(LookupKey key, final long value) {
        if (readOnly) throw new RuntimeException("Can not put in read only LookupData: " + hashPath);

        Long[] ref = new Long[1];
        writeCache.compute(key, (k, val) -> {
            if (val == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref[0] = null;
                    return value; // must write a new key with the value when we flush

                } else {
                    ref[0] = existingValue;

                    partitionLookupCache.putLookup(key, value); // Update the read cache

                    // This is gross - find a way to hold onto the position???
                    LookupMetadata lookupMetadata = partitionLookupCache.getMetadata(this);
                    Long pos = lookupMetadata.findKeyPosition(this, key);
                    keyLongBlobs.writeLong(pos, value); // Update the value on disk

                    // No need to add this to the write cache
                    return null;
                }
            } else {
                // This value is only in the write cache!
                ref[0] = val;
                return value;
            }
        });

        return ref[0];
    }

    private Long getCached(LookupKey key) {
        return partitionLookupCache.getLong(key, this::findValueFor);
    }

    /**
     * read the LookupKey by index
     * @param keyPosition the position in the longBlobs files
     * @return the cached lookup key
     */
    public LookupKey readKey(Long keyPosition) {
        return new LookupKey(keyLongBlobs.readBlob(keyPosition));
    }

    /**
     * Used in iterators to return an entry containing the key as a string and the value
     * @param keyPosition the position in the longBlobs files
     * @return the key and the long value associated with it
     */
    public Map.Entry<String, Long> readEntry(long keyPosition) {
        return Maps.immutableEntry(readKey(keyPosition).string(), readValue(keyPosition));
    }


    /**
     * Read the long value associated with a particular key number
     * @param keyPosition the position in the longBlobs files
     * @return the long value
     */
    public long readValue(long keyPosition){
        return keyLongBlobs.readLong(keyPosition);
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
        LookupMetadata lookupMetadata = partitionLookupCache.getMetadata(this);
        Long pos = lookupMetadata.findKeyPosition(this, key);
        if (pos == null){
            return null;
        } else {
            return readValue(pos);
        }
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

        log.debug("starting flush for {}", hashPath);
        if (writeCache.size() > 0) {


            Set<LookupKey> keys = writeCacheKeySetCopy();

            LookupMetadata currentMetadata = partitionLookupCache.getMetadata(this);

            // Now stream the keys and do sorted merge join on the keyStorageOrder from the current metadata

            int currentMetadataGeneration = currentMetadata.getMetadataGeneration();


            log.debug("Flushing {} entries for {}", keys.size(), hashPath);

            BulkAppender bulkBlobAppender = new BulkAppender(
                    keyLongBlobs, keys.stream().map(LookupKey::bytes).mapToInt(LongBlobStore::recordSize).sum()
            );

            Map<Integer, List<Map.Entry<Long, LookupKey>>> newKeysGroupedBySortOrderIndex;
            try {
                writeLock.lock();
                newKeysGroupedBySortOrderIndex = keys.stream()
                        .peek(key -> {
                            // Check the metadata generation of the LookupKeys
                            if (key.getMetaDataGeneration() != currentMetadataGeneration) {
                                // Update the index of the key which this new value sorts after
                                currentMetadata.findKeyPosition(this, key);
                            }
                        })
                        .map(key -> {
                                    long[] posValue = new long[1];
                                    writeCache.computeIfPresent(key, (k, v) -> {
                                        partitionLookupCache.putLookup(k, v);
                                        final byte[] keyBlob = LongBlobStore.byteRecord(v, key.bytes());
                                        final long pos = bulkBlobAppender.getBulkAppendPosition(keyBlob.length);
                                        posValue[0] = pos;
                                        bulkBlobAppender.addBulkAppendBytes(pos, keyBlob);
                                        return null;
                                    });

                                    return Maps.immutableEntry(posValue[0], key);
                                }

                        ).collect(Collectors.groupingBy(entry -> entry.getValue().getInsertAfterSortIndex(), Collectors.toList()));

                bulkBlobAppender.finishBulkAppend();

            } finally {
                writeLock.unlock();
            }

            log.debug("flushed keys for {}", hashPath);

            long[] currentKeySortOrder = currentMetadata.getKeyStorageOrder();

            long[] newKeySortOrder = new long[currentKeySortOrder.length + keys.size()];


            int index = 0;

            LookupKey minKey = currentMetadata.getMinKey();
            LookupKey maxKey = currentMetadata.getMaxKey();

            List<Map.Entry<Long, LookupKey>> newEntries = null;

            if(newKeysGroupedBySortOrderIndex.containsKey(-1)){
                newEntries = newKeysGroupedBySortOrderIndex.get(-1);
                newEntries.sort(Comparator.comparing(Map.Entry::getValue));

                minKey = newEntries.get(0).getValue();

                for(Map.Entry<Long, LookupKey> entry: newEntries){
                    newKeySortOrder[index] = entry.getKey();
                    index++;
                }
            }

            if (currentKeySortOrder.length == 0){
                if (newEntries == null) throw new RuntimeException("newKeys should never be null if currentKeysSortOrder is empty");
                maxKey = newEntries.get(newEntries.size() - 1).getValue();
            } else {
                for (long keyPosition : currentKeySortOrder) {
                    newKeySortOrder[index] = keyPosition;

                    if (newKeysGroupedBySortOrderIndex.containsKey(index)) {
                        newEntries = newKeysGroupedBySortOrderIndex.get(index);
                        newEntries.sort(Comparator.comparing(Map.Entry::getValue));

                        for (Map.Entry<Long, LookupKey> entry: newEntries) {
                            index++;
                            newKeySortOrder[index] = entry.getKey();

                            if (index == newKeySortOrder.length) {
                                maxKey = entry.getValue();
                            }
                        }

                    }
                    index++;
                }
            }

            log.debug("Finished creating sortOrder for {}", hashPath);

            partitionLookupCache.putMetadata(this,
                    LookupMetadata.generateMetadata(minKey, maxKey, newKeySortOrder, metadataPath, metaDataGeneration.incrementAndGet())
            );

            log.debug("flushed {}", hashPath);
        }
    }

//    Stream<LookupKey> keys() {
//        KeyIterator iter;
//        try {
//            readLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
//            iter = new KeyIterator(this);
//        } finally {
//            readLock.unlock();
//        }
//        Spliterator<LookupKey> spliter = Spliterators.spliterator(
//                iter,
//                iter.getNumKeys(),
//                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
//        );
//        return StreamSupport.stream(spliter, true);
//    }
//
//    Stream<Map.Entry<String, Long>> scan() {
//
//        KeyLongIterator iter;
//        try {
//            readLock.lock(); // Read lock the WriteCache while initializing the KeyLongIterator
//            iter = new KeyLongIterator(this);
//        } finally {
//            readLock.unlock();
//        }
//
//        Spliterator<Map.Entry<String, Long>> spliter = Spliterators.spliterator(
//                iter,
//                iter.getNumKeys(),
//                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
//        );
//        return StreamSupport.stream(spliter, true);
//    }
//
//    void scan(ObjLongConsumer<String> keyValueFunction) {
//        final int numKeys;
//        final Map<LookupKey, Long> writeCacheCopy;
//        try{
//            readLock.lock(); // Read lock the WriteCache while initializing the data to scan
//            numKeys = keyPosToBlockPos.getMaxIndex();
//
//            writeCacheCopy = writeCacheCopy();
//        } finally {
//            readLock.unlock();
//        }
//
//        writeCacheCopy
//                .forEach((key, value) -> keyValueFunction.accept(key.string(), value));
//
//        // Read but do not cache these keys - easy to add but is it helpful?
//        IntStream
//                .range(0, numKeys)
//                .mapToObj(this::readEntry)
//                .forEach(entry -> keyValueFunction
//                        .accept(entry.getKey(), entry.getValue()));
//    }
//
//    private static class KeyIterator implements Iterator<LookupKey> {
//
//        private int keyIndex = 0;
//        private final LookupData lookupData;
//        private final int maxKeyIndex;
//        private final int numKeys;
//        private final Iterator<LookupKey> writeCacheKeyIterator;
//
//        /**
//         * Should be constructed inside a ReadLock for the LookupData to ensure a consistent snapshot
//         * @param lookupData the keys to iterate
//         */
//        KeyIterator(LookupData lookupData) {
//            this.lookupData = lookupData;
//            // Get a snapshot of the keys
//            Set<LookupKey> writeCacheKeys = lookupData.writeCacheKeySetCopy();
//            maxKeyIndex = lookupData.keyPosToBlockPos.getMaxIndex();
//            numKeys = maxKeyIndex + writeCacheKeys.size();
//            writeCacheKeyIterator = writeCacheKeys.iterator();
//        }
//
//        int getNumKeys() {
//            return numKeys;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return keyIndex < numKeys;
//        }
//
//        @Override
//        public LookupKey next() {
//            LookupKey key;
//
//            if (keyIndex < maxKeyIndex){
//                key = lookupData.readKey(keyIndex); // Read but do not cache these keys - easy to add but is it helpful?
//            } else {
//                key = writeCacheKeyIterator.next();
//            }
//
//            keyIndex++;
//            return key;
//        }
//    }
//
//    private static class KeyLongIterator implements Iterator<Map.Entry<String, Long>> {
//
//        private int keyIndex = 0;
//        private final LookupData lookupData;
//        private final int maxKeyIndex;
//        private final int numKeys;
//        private final Iterator<Map.Entry<String, Long>> writeCacheKeyIterator;
//
//        /**
//         * Should be constructed inside a ReadLock for the LookupData to ensure a consistent snapshot
//         * @param lookupData the keys to iterate
//         */
//        KeyLongIterator(LookupData lookupData) {
//            this.lookupData = lookupData;
//            // Get a snapshot of the keys
//            Map<LookupKey, Long> writeCacheCopy = lookupData.writeCacheCopy();
//            maxKeyIndex = lookupData.keyPosToBlockPos.getMaxIndex();
//            numKeys = maxKeyIndex + writeCacheCopy.size();
//            writeCacheKeyIterator = writeCacheCopy
//                    .entrySet()
//                    .stream()
//                    .map(entry -> Maps.immutableEntry(entry.getKey().string(), entry.getValue()))
//                    .iterator();
//        }
//
//        int getNumKeys() {
//            return numKeys;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return keyIndex < numKeys;
//        }
//
//        @Override
//        public Map.Entry<String, Long>  next() {
//            Map.Entry<String, Long> result;
//
//            if (keyIndex < maxKeyIndex){
//                result = lookupData.readEntry(keyIndex); // Read but do not cache these keys - easy to add but is it helpful?
//            } else {
//                result = writeCacheKeyIterator.next();
//            }
//
//            keyIndex++;
//            return result;
//        }
//    }
}
