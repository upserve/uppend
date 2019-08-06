package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.metrics.LookupDataMetrics;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
/**
 * The bisect tree is linearized as follows
 *              8
 *          4
 *              9
 *      2
 *              10
 *          5
 *              11
 *  1
 *              12
 *          6
 *              13
 *      3
 *              14
 *          7
 *              15
 *  The size of the array containing the tree is 2^(n+1)
 *  If n is the current index the branch above is 2*n and the branch below is 2*n+1
 */
public class LookupMetadata {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MAX_BISECT_KEY_CACHE_DEPTH = 9; // Size == 1024
    private static final int MAX_TREE_NODES = treeSize(MAX_BISECT_KEY_CACHE_DEPTH);
    private final LookupKey[] bisectKeys = new LookupKey[MAX_TREE_NODES];

    private final int metadataGeneration;

    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final int[] keyStorageOrder;

    final LookupDataMetrics.Adders lookupDataMetricsAdders;
    final byte[] checksum;

    static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder,
                                                  VirtualMutableBlobStore metaDataBlobs, int metadataGeneration) {
        return generateMetadata(
                minKey, maxKey, keyStorageOrder, metaDataBlobs, metadataGeneration, new LookupDataMetrics.Adders()
        );
    }

    static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder,
                                                  VirtualMutableBlobStore metaDataBlobs, int metadataGeneration,
                                                  LookupDataMetrics.Adders lookupDataMetricsAdders) {
        LookupMetadata newMetadata = new LookupMetadata(
                minKey,
                maxKey,
                keyStorageOrder,
                metadataGeneration,
                lookupDataMetricsAdders
        );

        newMetadata.writeTo(metaDataBlobs);

        return newMetadata;
    }

    LookupMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metadataGeneration) {
        this(minKey, maxKey, keyStorageOrder, metadataGeneration, new LookupDataMetrics.Adders());

    }

    LookupMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metadataGeneration,
                   LookupDataMetrics.Adders lookupDataMetricsAdders) {
        this.numKeys = keyStorageOrder.length;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
        this.metadataGeneration = metadataGeneration;
        this.lookupDataMetricsAdders = lookupDataMetricsAdders;

        this.checksum = null;
    }

    public static LookupMetadata open(VirtualMutableBlobStore metadataBlobs, int metadataGeneration) {
        return open(metadataBlobs, metadataGeneration, null, new LookupDataMetrics.Adders());
    }

    public static LookupMetadata open(VirtualMutableBlobStore metadataBlobs, int metadataGeneration,
                                      LookupMetadata previous, LookupDataMetrics.Adders lookupDataMetricsAdders) {
        if (metadataBlobs.isPageAllocated(0L)) {
            byte[] currentChecksum = metadataBlobs.readChecksum(0L);

            // If the checksum has not changed return the previously LookupMetadata
            if (Objects.nonNull(previous) && Arrays.equals(currentChecksum, previous.checksum)) {
                return previous;
            } else {
                byte[] bytes = metadataBlobs.read(0L);
                return new LookupMetadata(bytes, metadataGeneration, currentChecksum, lookupDataMetricsAdders);
            }
        } else {
            return new LookupMetadata(null, null, new int[0], metadataGeneration, lookupDataMetricsAdders);
        }
    }

    private LookupMetadata(byte[] bytes, int metadataGeneration, byte[] checksum, LookupDataMetrics.Adders lookupDataMetricsAdders) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int minKeyLength, maxKeyLength;
        try {
            numKeys = buffer.getInt();
            minKeyLength = buffer.getInt();
            byte[] minKeyBytes = new byte[minKeyLength];
            buffer.get(minKeyBytes); // should check result - number of bytes read
            minKey = new LookupKey(minKeyBytes);
            maxKeyLength = buffer.getInt();
            byte[] maxKeyBytes = new byte[maxKeyLength];
            buffer.get(maxKeyBytes);
            maxKey = new LookupKey(maxKeyBytes);

            IntBuffer ibuf = buffer.asIntBuffer();
            keyStorageOrder = new int[numKeys];
            ibuf.get(keyStorageOrder);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Meta blob is corrupted", e); // The checksum is correct - indicates a format change!
        }

        this.metadataGeneration = metadataGeneration;
        this.checksum = checksum;
        this.lookupDataMetricsAdders = lookupDataMetricsAdders;
    }

    /**
     * Finds the value associated with a key or null if not present using bisect on the sorted storage order
     * If the result is null (key not found) the key is marked with the generation of the metadata used and the
     * sortIndex it should be inserted after.
     * If the result is not null (key was found) the key is marked with its position in the longBlob file.
     *
     * @param longBlobStore The longBlobStore to read keys and values
     * @param key the key to find and mark
     * @return the position of the key
     */
    Long findKey(VirtualLongBlobStore longBlobStore, LookupKey key) {
        // Use a try finally block to time execution
        // https://softwareengineering.stackexchange.com/questions/210428/is-try-finally-expensive
        final long tic = System.nanoTime();
        try {
            key.setMetaDataGeneration(metadataGeneration);

            if (numKeys == 0) {
                key.setInsertAfterSortIndex(-1);
                lookupDataMetricsAdders.lookupMissCount.increment();
                return null;
            }

            int keyIndexLower = 0;
            int keyIndexUpper = numKeys - 1;
            LookupKey lowerKey = minKey;
            LookupKey upperKey = maxKey;

            int bisectCount = 0;
            int bisectKeyTreeArrayIndex = 1;

            int keyPosition;
            LookupKey midpointKey;
            int midpointKeyIndex;

            int comparison = lowerKey.compareTo(key);
            if (comparison > 0 /* new key is less than lowerKey */) {
                key.setInsertAfterSortIndex(-1); // Insert it after this index in the sort order
                lookupDataMetricsAdders.lookupMissCount.increment();
                return null;
            }
            if (comparison == 0) {
                key.setPosition(keyStorageOrder[keyIndexLower]);
                lookupDataMetricsAdders.lookupHitCount.increment();
                return longBlobStore.readLong(keyStorageOrder[keyIndexLower]);
            }

            comparison = upperKey.compareTo(key);
            if (comparison < 0 /* new key is greater than upperKey */) {
                key.setInsertAfterSortIndex(keyIndexUpper); // Insert it after this index in the sort order
                lookupDataMetricsAdders.lookupMissCount.increment();
                return null;
            }
            if (comparison == 0) {
                key.setPosition(keyStorageOrder[keyIndexUpper]);
                lookupDataMetricsAdders.lookupHitCount.increment();
                return longBlobStore.readLong(keyStorageOrder[keyIndexUpper]);
            }

            if (numKeys == 2) { // There are no other values keys besides upper and lower
                key.setInsertAfterSortIndex(keyIndexLower);
                lookupDataMetricsAdders.lookupMissCount.increment();
                return null;
            }

            // bisect till we find the key or return null
            do {
                midpointKeyIndex = keyIndexLower + ((keyIndexUpper - keyIndexLower) / 2);

                if (log.isTraceEnabled())
                    log.trace("reading {}: [{}, {}], [{}, {}], {}", key, keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);

                keyPosition = keyStorageOrder[midpointKeyIndex];
                // Cache only the most frequently used midpoint keys
                if (bisectCount < MAX_BISECT_KEY_CACHE_DEPTH) {
                    if (bisectKeys[bisectKeyTreeArrayIndex] == null) {
                        lookupDataMetricsAdders.cacheMissCount.increment();
                        midpointKey = bisectKeys[bisectKeyTreeArrayIndex] = new LookupKey(longBlobStore.readBlob(keyPosition));
                    } else {
                        lookupDataMetricsAdders.cacheHitCount.increment();
                        midpointKey = bisectKeys[bisectKeyTreeArrayIndex];
                    }
                } else {
                    midpointKey = new LookupKey(longBlobStore.readBlob(keyPosition));
                }

                comparison = key.compareTo(midpointKey);

                if (comparison == 0) {
                    key.setPosition(keyPosition);
                    lookupDataMetricsAdders.lookupHitCount.increment();
                    return longBlobStore.readLong(keyPosition);
                }

                if (comparison < 0) {
                    upperKey = midpointKey;
                    keyIndexUpper = midpointKeyIndex;
                    bisectKeyTreeArrayIndex = bisectKeyTreeArrayIndex * 2;

                } else {
                    lowerKey = midpointKey;
                    keyIndexLower = midpointKeyIndex;
                    bisectKeyTreeArrayIndex = bisectKeyTreeArrayIndex * 2 + 1;
                }

                bisectCount++;
            } while ((keyIndexLower + 1) < keyIndexUpper);

            key.setInsertAfterSortIndex(keyIndexLower); // Insert it in the sort order after this key
            lookupDataMetricsAdders.lookupMissCount.increment();
            return null;
        }
        finally {
            lookupDataMetricsAdders.findKeyTimer.add(System.nanoTime() - tic);
        }
    }

    public static int treeSize(int depth) {
        return 1 << (depth +1);
    }

    void clearLookupTree(){
        Arrays.fill(bisectKeys, null);
    }

    public void writeTo(VirtualMutableBlobStore metadataBlobs) {
        int headerSize = 12 + minKey.byteLength() + maxKey.byteLength();
        int intBufSize = 4 * numKeys;
        ByteBuffer byteBuffer = ByteBuffer.allocate(headerSize + intBufSize);
        byteBuffer.putInt(numKeys);
        byteBuffer.putInt(minKey.byteLength());
        byteBuffer.put(minKey.bytes());
        byteBuffer.putInt(maxKey.byteLength());
        byteBuffer.put(maxKey.bytes());

        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(keyStorageOrder);
        byteBuffer.rewind();

        metadataBlobs.write(0L, byteBuffer.array());
    }

    @Override
    public String toString() {
        return "LookupMetadata{" +
                "numKeys=" + numKeys +
                ", minKey=" + minKey +
                ", maxKey=" + maxKey +
                '}';
    }

    public int getMetadataGeneration() {
        return metadataGeneration;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public int[] getKeyStorageOrder() {
        return keyStorageOrder;
    }

    public LookupKey getMinKey() {
        return minKey;
    }

    public LookupKey getMaxKey() {
        return maxKey;
    }
}
