package com.upserve.uppend.lookup;

import com.google.common.collect.Maps;
import com.upserve.uppend.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.metrics.LookupDataMetrics;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.util.stream.*;

public class LookupData implements Flushable, Trimmable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Random random = new Random();

    private final AtomicInteger writeCacheCounter;
    private final AtomicBoolean flushing;
    private final AtomicBoolean firstFlush;

    private final int flushThreshold;
    private final int firstFlushThreshold;
    private final int reloadInterval;  // Reload interval is specified in seconds

    // The container for stuff we need to write - Only new keys can be in the write cache
    final ConcurrentHashMap<LookupKey, Long> writeCache;
    // keys written but not yet in the metadata live here
    final ConcurrentHashMap<LookupKey, Long> flushCache;

    // Direct reference for writers
    private AtomicReference<LookupMetadata> atomicMetadataRef;

    // Timestamped references for readers
    final AtomicStampedReference<LookupMetadata> timeStampedMetadata; // removed 'private' to support unit testing
    final AtomicInteger reloadStamp; // removed 'private' to support unit testing
    private final long startTime;

    private final boolean readOnly;

    private final VirtualLongBlobStore keyLongBlobs;

    private final VirtualMutableBlobStore metadataBlobs;

    private final ReadWriteLock consistentWriteCacheLock;
    private final Lock consistentWriteCacheReadLock;
    private final Lock consistentWriteCacheWriteLock;

    // Flushing every 30 seconds, we can run for 2000 years before the metaDataGeneration hits INTEGER.MAX_VALUE
    private AtomicInteger metaDataGeneration;

    final LookupDataMetrics.Adders lookupDataMetricsAdders;


    public static LookupData lookupWriter(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs,
                                          int flushThreshold){
        return lookupWriter(keyLongBlobs, metadataBlobs, flushThreshold, new LookupDataMetrics.Adders());
    }

    public static LookupData lookupWriter(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs,
                                          int flushThreshold, LookupDataMetrics.Adders lookupDataMetricsAdders){
        return new LookupData(
                keyLongBlobs, metadataBlobs, flushThreshold, -1, false, lookupDataMetricsAdders
        );
    }

    public static LookupData lookupReader(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs,
                                          int reloadInterval){
        return lookupReader(keyLongBlobs, metadataBlobs, reloadInterval, new LookupDataMetrics.Adders());
    }

    public static LookupData lookupReader(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs,
                                          int reloadInterval, LookupDataMetrics.Adders lookupDataMetricsAdders){
        return new LookupData(
                keyLongBlobs, metadataBlobs, -1, reloadInterval, true, lookupDataMetricsAdders
        );
    }

    /**
     * The Constructor (TM).
     *
     * @param keyLongBlobs storage for keys and associated long values
     * @param metadataBlobs lexically sorted key index
     * @param flushThreshold number of keys that trigger scheduling of flush; use n == -1 to disable
     * @param reloadInterval (for read-only mode) number of seconds to metadata to expire, reload is immediate for
     *                       the first thread that hits it; use n <= 0 to disable
     * @param readOnly a very self-descriptive boolean value
     * @param lookupDataMetricsAdders thread-safe timing and metrics container
     */
    private LookupData(VirtualLongBlobStore keyLongBlobs, VirtualMutableBlobStore metadataBlobs, int flushThreshold,
                       int reloadInterval, boolean readOnly, LookupDataMetrics.Adders lookupDataMetricsAdders) {
        this.keyLongBlobs = keyLongBlobs;
        this.metadataBlobs = metadataBlobs;

        this.readOnly = readOnly;
        this.lookupDataMetricsAdders = lookupDataMetricsAdders;

        this.firstFlush = new AtomicBoolean(true);
        this.firstFlushThreshold = flushThreshold *  (random.nextInt(100) + 25) / 100;
        this.flushing = new AtomicBoolean(false);
        this.flushThreshold = flushThreshold;
        this.reloadInterval = reloadInterval;

        // Record the time we started this LookupData
        startTime = System.currentTimeMillis();

        writeCacheCounter = new AtomicInteger();
        metaDataGeneration = new AtomicInteger();

        atomicMetadataRef = new AtomicReference<>();

        if (readOnly) {
            writeCache = null;
            flushCache = null;

            // Reload interval is specified in seconds
            timeStampedMetadata = new AtomicStampedReference<>(loadMetadata(), reloadInterval);
            reloadStamp = new AtomicInteger(reloadInterval);
        } else {
            atomicMetadataRef.set(loadMetadata());

            timeStampedMetadata = null;
            reloadStamp = null;

            writeCache = new ConcurrentHashMap<>();
            flushCache = new ConcurrentHashMap<>();
        }

        consistentWriteCacheLock = new ReentrantReadWriteLock();
        consistentWriteCacheReadLock = consistentWriteCacheLock.readLock();
        consistentWriteCacheWriteLock = consistentWriteCacheLock.writeLock();
    }

    /**
     * Return the value associated with the given key
     *
     * @param key the key to look up
     * @return the value associated with the key, null if the key was not found
     */
    public Long getValue(LookupKey key) {
        if (readOnly) {
            return findValueFor(key);
        } else {
            Long result = writeCache.get(key);

            if (result == null) {
                return findValueFor(key);
            } else {
                return result;
            }
        }
    }

    VirtualMutableBlobStore getMetadataBlobs() {
        return metadataBlobs;
    }

    private void flushThreshold() {
        if (flushThreshold == -1) return;

        if (shouldFlush(writeCacheCounter.getAndIncrement())) {
            AutoFlusher.submitWork(this::flush);
        }
    }

    private boolean shouldFlush(int writeCount) {
        if (!flushing.get() && firstFlush.get() && writeCount == firstFlushThreshold) {
            flushing.set(true);
            firstFlush.set(false);
            return true;
        } else if (!flushing.get() && writeCount == flushThreshold){
            flushing.set(true);
            return true;
        } else {
            return false;
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
            if (value == null) {
                Long existingValue = findValueFor(k);
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
            if (val == null) {
                Long existingValue = findValueFor(k);
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
        if (readOnly) throw new RuntimeException("Can not increment in read only LookupData");

        long[] ref = new long[1];
        writeCache.compute(key, (writeKey, value) -> {
            if (value == null) {
                Long existingValue = findValueFor(writeKey);
                if (existingValue == null) {
                    ref[0] = delta;
                    flushThreshold();
                    return delta; // must write a new key with delta as the value when we flush

                } else {
                    long newValue = existingValue + delta;
                    ref[0] = newValue;

                    flushCache.computeIfPresent(writeKey, (flushKey, v) -> newValue);
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
            if (val == null) {
                Long existingValue = findValueFor(writeKey);
                if (existingValue == null) {
                    ref[0] = null;
                    flushThreshold();
                    return value; // must write a new key with the value when we flush

                } else {
                    ref[0] = existingValue;

                    flushCache.computeIfPresent(writeKey, (flushKey, v) -> value);
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

    /**
     * read the LookupKey by index
     *
     * @param keyPosition the position in the longBlobs files
     * @return the cached lookup key
     */
    public LookupKey readKey(Long keyPosition) {
        return new LookupKey(keyLongBlobs.readBlob(keyPosition));
    }

    /**
     * Used in iterators to return an entry containing the key as a string and the value
     *
     * @param keyPosition the position in the longBlobs files
     * @return the key and the long value associated with it
     */
    public Map.Entry<LookupKey, Long> readEntry(long keyPosition) {
        return Maps.immutableEntry(readKey(keyPosition), readValue(keyPosition));
    }

    /**
     * Read the long value associated with a particular key number
     *
     * @param keyPosition the position in the longBlobs files
     * @return the long value
     */
    public long readValue(long keyPosition) {
        return keyLongBlobs.readLong(keyPosition);
    }

    public long getMetadataSize(){
        return getMetadata().getNumKeys();
    }

    /**
     * Load a key from cached pages
     *
     * @param key the key we are looking for
     * @return Long value or null if not present
     */
    private Long findValueFor(LookupKey key) {
        if (!readOnly) {
            Long result = flushCache.get(key);
            if (result != null ){
                return result;
            }
        }
        LookupMetadata md = getMetadata();
        return md.findKey(keyLongBlobs, key);
    }

    LookupMetadata loadMetadata() {
        return loadMetadata(null);
    }

    LookupMetadata loadMetadata(LookupMetadata lookupMetadata) {
        try {
            return LookupMetadata.open(
                    getMetadataBlobs(),
                    getMetaDataGeneration(),
                    lookupMetadata,
                    lookupDataMetricsAdders
            );
        } catch (IllegalStateException e) {
            if (readOnly) {
                log.warn("getMetaData failed for read only store - attempting to reload!", e);
                // Try again and let the exception bubble if it fails
                return LookupMetadata.open(
                        getMetadataBlobs(),
                        getMetaDataGeneration(),
                        lookupMetadata,
                        lookupDataMetricsAdders
                );
            }
            // `else` statement not needed because of the return statement above
            log.warn("getMetaData failed for read write store - attempting to repair it!", e);
            return repairMetadata();
        }
    }

    private synchronized LookupMetadata repairMetadata() {
        int[] sortedPositions = keyLongBlobs.positionBlobStream()
                .sorted(Comparator.comparing(entry -> new LookupKey(entry.getValue())))
                .mapToInt(entry -> entry.getKey().intValue())
                .toArray();

        int sortedPositionsSize = sortedPositions.length;
        LookupKey minKey = sortedPositionsSize > 0 ? readKey((long) sortedPositions[0]) : null;
        LookupKey maxKey = sortedPositionsSize > 0 ? readKey((long) sortedPositions[sortedPositionsSize - 1]) : null;
        return LookupMetadata.generateMetadata(minKey, maxKey, sortedPositions, metadataBlobs,
                metaDataGeneration.incrementAndGet(), lookupDataMetricsAdders);
    }

    private int getMetaDataGeneration() {
        return metaDataGeneration.get();
    }

    /**
     * Create a copy of the keys currently in the write cache
     *
     * @return the key set
     */
    private Set<LookupKey> writeCacheKeySetCopy() {
        if (writeCache != null) {
            return new HashSet<>(writeCache.keySet());
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Create a copy of the write cache
     *
     * @return the copy of the Map
     */
    private Map<LookupKey, Long> writeCacheCopy() {
        if (writeCache != null) {
            return new HashMap<>(writeCache);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Only counts flushed keys.
     *
     * @return the number of keys
     */
    public int keyCount() {
        return getMetadata().getNumKeys();
    }

    void flushWriteCache(LookupMetadata currentMetadata) {

        Set<LookupKey> keys = writeCacheKeySetCopy();

        // Now stream the keys and do sorted merge join on the keyStorageOrder from the current metadata

        int currentMetadataGeneration = currentMetadata.getMetadataGeneration();
        log.debug("Flushing {} entries", keys.size());

        try {
            // Write lock while we move entries from the writeCache to the flush cache
            // This does not block new inserts - only scan operations that need a consistent view of the flushCache and writeCache
            consistentWriteCacheWriteLock.lock();
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

                                    if (k.byteLength() > 256) log.warn("Key length greater than 256: {}", key.toString());
                                    final long pos = keyLongBlobs.append(v, k.bytes());
                                    if (pos > Integer.MAX_VALUE) {
                                        // TODO is there a way to deal with this?
                                        throw new IllegalStateException("Maximum key store size exceeded!");
                                    }
                                    key.setPosition((int) pos);

                                    return null;
                                });
                            }
                    );

        } finally {
            consistentWriteCacheWriteLock.unlock();
        }

        log.debug("flushed keys");
    }

    void generateMetaData(LookupMetadata currentMetadata) {
        int[] currentKeySortOrder = currentMetadata.getKeyStorageOrder();

        int flushSize = flushCache.size();

        // Increment the stats here
        lookupDataMetricsAdders.flushedKeyCounter.add(flushSize);
        lookupDataMetricsAdders.flushCounter.increment();

        // Update the counter and flush again if there are still more entries in the write cache than the threshold
        if (flushThreshold != -1 && writeCacheCounter.addAndGet(-flushSize) > flushThreshold) {
            AutoFlusher.submitWork(this::flush);
        }

        int[] newKeySortOrder = new int[currentKeySortOrder.length + flushSize];

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

            for (LookupKey key : newEntries) {
                newKeySortOrder[index] = key.getPosition();
                index++;
            }

            if (i == currentKeySortOrder.length - 1 && newEntries.size() > 0) {
                maxKey = newEntries.get(newEntries.size() - 1);
            }
        }

        log.debug("Finished creating sortOrder");

        atomicMetadataRef.set(
                LookupMetadata
                        .generateMetadata(
                                minKey,
                                maxKey,
                                newKeySortOrder,
                                metadataBlobs,
                                metaDataGeneration.incrementAndGet(),
                                lookupDataMetricsAdders
                        )
        );
    }

    protected LookupMetadata getMetadata() {
        if (readOnly){
            int[] stamp = new int[1];
            LookupMetadata result = timeStampedMetadata.get(stamp);
            // Convert millis to seconds
            if (reloadInterval > 0 && (System.currentTimeMillis() - startTime / 1000) > stamp[0]){
                // a reloadInterval of 0 prevents reloading of the metadata
                boolean reloadMetadata = reloadStamp.compareAndSet(stamp[0], stamp[0] + reloadInterval);
                if (reloadMetadata) {
                    log.warn("Loading metadata");
                    result = loadMetadata(result);
                    timeStampedMetadata.set(result, stamp[0] + reloadInterval);
                }
            }
            return result;
        } else {
            return atomicMetadataRef.get();
        }
    }

    @Override
    public synchronized void flush() {
        if (readOnly) throw new RuntimeException("Can not flush read only LookupData");

        if (writeCache.size() > 0) {
            final long tic = System.nanoTime();
            flushing.set(true);
            log.debug("starting flush");

            LookupMetadata md = atomicMetadataRef.get();
            flushWriteCache(md);

            generateMetaData(md);

            flushCache.clear();

            log.debug("flushed");
            lookupDataMetricsAdders.flushTimer.add(System.nanoTime() - tic);
        }
        flushing.set(false);
    }

    @Override
    public void trim() {
        if (!readOnly) {
            flush();
        } else {
            int[] stamp = new int[1];
            LookupMetadata result = timeStampedMetadata.get(stamp);
            result = loadMetadata(result);
            timeStampedMetadata.set(result, stamp[0]);
            reloadStamp.set(stamp[0]);
        }
    }

    private int[] getKeyPosition() {
        if (readOnly) {
            return getMetadata().getKeyStorageOrder();
        } else {
            return IntStream.concat(
                    flushCache.keySet().stream().mapToInt(LookupKey::getPosition),
                    Arrays.stream(getMetadata().getKeyStorageOrder())
            ).distinct().toArray();
        }
    }

    public Stream<LookupKey> keys() {
        LookupDataIterator<LookupKey> iter;
        try {
            consistentWriteCacheReadLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
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
            consistentWriteCacheReadLock.unlock();
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
            consistentWriteCacheReadLock.lock(); // Read lock the WriteCache while initializing the KeyIterator
            Map<LookupKey, Long> map = writeCacheCopy();
            iter = new LookupDataIterator<>(
                    getKeyPosition(),
                    map.size(),
                    map.entrySet().stream().map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue())).iterator(),
                    this::readEntry
            );
        } finally {
            consistentWriteCacheReadLock.unlock();
        }

        Spliterator<Map.Entry<LookupKey, Long>> spliter = Spliterators.spliterator(
                iter,
                iter.getNumKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SIZED
        );
        return StreamSupport.stream(spliter, true);
    }

    public void scan(BiConsumer<LookupKey, Long> keyValueFunction) {
        final int[] positions;
        final Map<LookupKey, Long> writeCacheCopy;
        try {
            consistentWriteCacheReadLock.lock(); // Read lock the WriteCache while initializing the data to scan
            positions = getKeyPosition();

            writeCacheCopy = writeCacheCopy();
        } finally {
            consistentWriteCacheReadLock.unlock();
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
