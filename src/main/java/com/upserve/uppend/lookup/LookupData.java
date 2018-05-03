package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.AutoFlusher;
import com.upserve.uppend.blobs.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;

public class LookupData implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final PartitionLookupCache partitionLookupCache;

    private final AtomicInteger writeCacheCounter;

    private final int flushThreshold;

    // The container for stuff we need to write - Only new keys can be in the write cache
    final ConcurrentHashMap<LookupKey, Long> writeCache;
    // keys written but not yet written to the metadata live here
    final ConcurrentHashMap<LookupKey, Long> flushCache;

    private final boolean readOnly;

    private final VirtualLongBlobStore keyLongBlobs;

    private final VirtualMutableBlobStore metadataBlobs;

    private final ReadWriteLock flushLock;
    private final Lock readLock;
    private final Lock writeLock;

    // Flushing every 30 seconds, we can run for 2000 years before the metaDataGeneration hits INTEGER.MAX_VALUE
    private AtomicInteger metaDataGeneration;

    public LookupData(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs, PartitionLookupCache lookupCache, boolean readOnly) {
        this(keyLongBlobs, metadataBlobs, lookupCache, -1, readOnly);
    }

    public LookupData(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs, PartitionLookupCache lookupCache, int flushThreshold, boolean readOnly) {

        this.keyLongBlobs = keyLongBlobs;
        this.metadataBlobs = metadataBlobs;

        this.partitionLookupCache = lookupCache;

        this.readOnly = readOnly;

        this.flushThreshold = flushThreshold;

        if (readOnly) {
            writeCache = null;
            flushCache = null;
        } else {
            writeCache = new ConcurrentHashMap<>();
            flushCache = new ConcurrentHashMap<>();
        }

        writeCacheCounter = new AtomicInteger();

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
    public Long getValue(LookupKey key) {
        if (readOnly) {
            return getCached(key);
        } else {
            return writeCache.getOrDefault(key, getCached(key));
        }
    }

    VirtualMutableBlobStore getMetadataBlobs(){
        return metadataBlobs;
    }

    private void flushThreshold(){
        if (flushThreshold != -1 && writeCacheCounter.getAndIncrement() == flushThreshold) {
            AutoFlusher.flusherWorkPool.submit(this::flush);
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
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData");

        long[] ref = new long[1];
        writeCache.compute(key, (k, value) -> {
            if (value == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    long val = allocateLongFunc.getAsLong();
                    ref[0] = val;
                    flushThreshold();
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
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData");

        long[] ref = new long[1];
        writeCache.compute(key, (k, val) -> {
            if (val == null){
                Long existingValue = getCached(k);
                if (existingValue == null) {
                    ref[0] = value;
                    flushThreshold();
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
        if (readOnly) throw new RuntimeException("Can not putIfNotExists in read only LookupData");

        long[] ref = new long[1];
        writeCache.compute(key, (writeKey, value) -> {
            if (value == null){
                Long existingValue = getCached(writeKey);
                if (existingValue == null) {
                    ref[0] = delta;
                    flushThreshold();
                    return delta; // must write a new key with delta as the value when we flush

                } else {
                    long newValue = existingValue + delta;
                    ref[0] = newValue;

                    flushCache.computeIfPresent(writeKey, (flushKey, v) -> newValue);
                    partitionLookupCache.putLookup(writeKey, newValue); // Update the read cache
                    keyLongBlobs.writeLong(writeKey.getPosition(), newValue); // Update the value on disk

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
        if (readOnly) throw new RuntimeException("Can not put in read only LookupData");

        Long[] ref = new Long[1];
        writeCache.compute(key, (writeKey, val) -> {
            if (val == null){
                Long existingValue = getCached(writeKey);
                if (existingValue == null) {
                    ref[0] = null;
                    flushThreshold();
                    return value; // must write a new key with the value when we flush

                } else {
                    ref[0] = existingValue;

                    flushCache.computeIfPresent(writeKey, (flushKey, v) -> value);
                    partitionLookupCache.putLookup(writeKey, value); // Update the read cache
                    keyLongBlobs.writeLong(writeKey.getPosition(), value); // Update the value on disk

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
        if (readOnly){
            return partitionLookupCache.getLong(key, this::findValueFor);
        } else {
            return flushCache.getOrDefault(key, partitionLookupCache.getLong(key, this::findValueFor));
        }
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
    public Map.Entry<LookupKey, Long> readEntry(long keyPosition) {
        return Maps.immutableEntry(readKey(keyPosition), readValue(keyPosition));
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
        return lookupMetadata.findKey(keyLongBlobs, key);
    }

    int getMetaDataGeneration(){
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

    void flushWriteCache(LookupMetadata currentMetadata){

        Set<LookupKey> keys = writeCacheKeySetCopy();

        // Now stream the keys and do sorted merge join on the keyStorageOrder from the current metadata

        int currentMetadataGeneration = currentMetadata.getMetadataGeneration();
        log.debug("Flushing {} entries", keys.size());

        try {
            // Write lock while we move entries from the writeCache to the flush cache
            // This does not block new inserts - only scan operations that need a consistent view of the flushCache and writeCache
            writeLock.lock();
            keys.stream()
                    .peek(key -> {
                        // Check the metadata generation of the LookupKeys
                        if (key.getMetaDataGeneration() != currentMetadataGeneration) {
                            // Update the index of the key for the current metadata generation for so we can insert it correctly
                            currentMetadata.findKey(keyLongBlobs, key);
                        }
                    })
                    .forEach(key -> {
                                writeCache.computeIfPresent(key, (k, v) -> {
                                    flushCache.put(k, v);

                                    final long pos = keyLongBlobs.append(v, k.bytes());
                                    key.setPosition(pos);

                                    return null;
                                });
                            }

                    );

        } finally {
            writeLock.unlock();
        }

        log.debug("flushed keys");

    }

    void generateMetaData(LookupMetadata currentMetadata){
        long[] currentKeySortOrder = currentMetadata.getKeyStorageOrder();

        int flushSize = flushCache.size();

        // Update the counter and flush again if there are still more entries in the write cache than the threshold
        if (flushThreshold != -1 && writeCacheCounter.addAndGet(-flushSize) > flushThreshold){
            AutoFlusher.flusherWorkPool.submit(this::flush);
        }

        long[] newKeySortOrder = new long[currentKeySortOrder.length + flushSize];

        Map<Integer, List<LookupKey>> newKeysGroupedBySortOrderIndex = flushCache.keySet().stream().collect(Collectors.groupingBy(LookupKey::getInsertAfterSortIndex, Collectors.toList()));

        int index = 0;

        LookupKey minKey = currentMetadata.getMinKey();
        LookupKey maxKey = currentMetadata.getMaxKey();

        List<LookupKey> newEntries = null;

        for (int i = -1; i < currentKeySortOrder.length; i++) {
            newEntries = newKeysGroupedBySortOrderIndex.getOrDefault(i, Collections.emptyList());
            newEntries.sort(LookupKey::compareTo);

            if (i == -1) {
                if (newEntries.size() > 0) minKey = newEntries.get(0);
            } else {
                newKeySortOrder[index] = currentKeySortOrder[i];
                index++;
            }

            for(LookupKey key: newEntries){
                newKeySortOrder[index] = key.getPosition();
                index++;
            }

            if (i == currentKeySortOrder.length - 1 && newEntries.size() > 0){
                maxKey = newEntries.get(newEntries.size() - 1);
            }
        }

        log.debug("Finished creating sortOrder");

        try {
            LookupMetadata metadata = LookupMetadata.generateMetadata(minKey, maxKey, newKeySortOrder, metadataBlobs, metaDataGeneration.incrementAndGet());
            partitionLookupCache.putMetadata(this, metadata);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write new metadata!", e);
        }
    }

    void flushCacheToReadCache() {
        Iterator<Map.Entry<LookupKey, Long>> iterator = flushCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<LookupKey, Long> entry = iterator.next();
            partitionLookupCache.putLookup(entry.getKey(), entry.getValue());
            iterator.remove();
        }
    }


    @Override
    public synchronized void flush() {
        if (readOnly) throw new RuntimeException("Can not flush read only LookupData");

        if (writeCache.size() > 0) {
            log.debug("starting flush");

            LookupMetadata currentMetadata = partitionLookupCache.getMetadata(this);

            flushWriteCache(currentMetadata);

            generateMetaData(currentMetadata);

            flushCacheToReadCache();
            log.debug("flushed");
        }
    }

    public void clear(){
        writeCache.clear();
        flushCache.clear();
    }

    private long[] getKeyPosition() {
        if (readOnly) {
            return partitionLookupCache.getMetadata(this).getKeyStorageOrder();
        } else {
            return LongStream.concat(
                    flushCache.keySet().stream().mapToLong(LookupKey::getPosition),
                    Arrays.stream(partitionLookupCache.getMetadata(this).getKeyStorageOrder())
            ).distinct().toArray();
        }
    }

    public Stream<LookupKey> keys() {
        LookupDataIterator<LookupKey> iter;
        try {
            readLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
            Set<LookupKey> keySet = writeCacheKeySetCopy();
            iter = new LookupDataIterator<>(
                    getKeyPosition(),
                    keySet.size(),
                    keySet.iterator(),
                    position -> {
                        LookupKey key = readKey(position);
                        return key;
                }
            );
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

    public Stream<Map.Entry<LookupKey, Long>> scan() {

        LookupDataIterator<Map.Entry<LookupKey, Long>> iter;
        try {
            readLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
            Map<LookupKey, Long> map = writeCacheCopy();
            iter = new LookupDataIterator<>(
                    getKeyPosition(),
                    map.size(),
                    map.entrySet().stream().map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue())).iterator(),
                    this::readEntry
            );
        } finally {
            readLock.unlock();
        }

        Spliterator<Map.Entry<LookupKey, Long>> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true);
    }

    public void scan(BiConsumer<LookupKey, Long> keyValueFunction) {
        final long[] positions;
        final Map<LookupKey, Long> writeCacheCopy;
        try{
            readLock.lock(); // Read lock the WriteCache while initializing the data to scan
            positions = getKeyPosition();

            writeCacheCopy = writeCacheCopy();
        } finally {
            readLock.unlock();
        }

        writeCacheCopy
                .forEach(keyValueFunction);

        // Read but do not cache these keys - easy to add but is it helpful?
        Arrays.stream(positions)
                .parallel()
                .mapToObj(this::readEntry)
                .forEach(entry -> keyValueFunction
                        .accept(entry.getKey(), entry.getValue()));
    }

}
