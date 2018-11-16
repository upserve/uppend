package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.util.concurrent.ConcurrentHashMap;
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

    final LongAdder hitCount;
    final LongAdder missCount;
    public static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, VirtualMutableBlobStore metaDataBlobs, int metadataGeneration, LongAdder missCount, LongAdder hitCount) throws IOException {

        LookupMetadata newMetadata = new LookupMetadata(
                minKey,
                maxKey,
                keyStorageOrder,
                metadataGeneration,
                missCount,
                hitCount
        );

        newMetadata.writeTo(metaDataBlobs);

        return newMetadata;
    }

    LookupMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metadataGeneration) {
        this(minKey, maxKey, keyStorageOrder, metadataGeneration, new LongAdder(), new LongAdder());
    }


    private LookupMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metadataGeneration, LongAdder missCount, LongAdder hitCount) {
        this.numKeys = keyStorageOrder.length;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
        this.metadataGeneration = metadataGeneration;

        this.hitCount = hitCount;
        this.missCount = missCount;

    }

    public static LookupMetadata open(VirtualMutableBlobStore metadataBlobs, int metadataGeneration) {
        return open(metadataBlobs, metadataGeneration, new LongAdder(), new LongAdder());
    }

    public static LookupMetadata open(VirtualMutableBlobStore metadataBlobs, int metadataGeneration, LongAdder missCount, LongAdder hitCount) {
        if (metadataBlobs.isPageAllocated(0L)) {
            byte[] bytes = metadataBlobs.read(0L);
            return new LookupMetadata(bytes, metadataGeneration, missCount, hitCount);
        } else {
            return new LookupMetadata(null, null, new int[0], metadataGeneration, missCount, hitCount);
        }
    }

    private LookupMetadata(byte[] bytes, int metadataGeneration, LongAdder missCount, LongAdder hitCount) {
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

        this.hitCount = hitCount;
        this.missCount = missCount;
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

    public Long findKey(VirtualLongBlobStore longBlobStore, LookupKey key) {

        key.setMetaDataGeneration(metadataGeneration);

        if (numKeys == 0) {
            key.setInsertAfterSortIndex(-1);
            missCount.increment();
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
            missCount.increment();
            return null;
        }
        if (comparison == 0) {
            key.setPosition(keyStorageOrder[keyIndexLower]);
            hitCount.increment();
            return longBlobStore.readLong(keyStorageOrder[keyIndexLower]);
        }

        comparison = upperKey.compareTo(key);
        if (comparison < 0 /* new key is greater than upperKey */) {
            key.setInsertAfterSortIndex(keyIndexUpper); // Insert it after this index in the sort order
            missCount.increment();
            return null;
        }
        if (comparison == 0) {
            key.setPosition(keyStorageOrder[keyIndexUpper]);
            hitCount.increment();
            return longBlobStore.readLong(keyStorageOrder[keyIndexUpper]);
        }

        if (numKeys == 2) { // There are no other values keys besides upper and lower
            key.setInsertAfterSortIndex(keyIndexLower);
            missCount.increment();
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
                if (bisectKeys[bisectKeyTreeArrayIndex] == null){
                    midpointKey = bisectKeys[bisectKeyTreeArrayIndex] = new LookupKey(longBlobStore.readBlob(keyPosition));
                } else {
                    midpointKey = bisectKeys[bisectKeyTreeArrayIndex];
                }
            } else {
                midpointKey = new LookupKey(longBlobStore.readBlob(keyPosition));
            }

            comparison = key.compareTo(midpointKey);
            if (comparison < 0) {
                upperKey = midpointKey;
                keyIndexUpper = midpointKeyIndex;

                bisectKeyTreeArrayIndex = bisectKeyTreeArrayIndex * 2;

            } else if (comparison > 0) {
                keyIndexLower = midpointKeyIndex;
                lowerKey = midpointKey;

                bisectKeyTreeArrayIndex = bisectKeyTreeArrayIndex * 2 + 1;
            } else {
                key.setPosition(keyPosition);
                hitCount.increment();
                return longBlobStore.readLong(keyPosition);
            }

            bisectCount++;
        } while ((keyIndexLower + 1) < keyIndexUpper);

        key.setInsertAfterSortIndex(keyIndexLower); // Insert it in the sort order after this key
        missCount.increment();
        return null;
    }

    public static int treeSize(int depth) {
        return 1 << (depth +1);
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

    /**
     * Size of keyStorageOrder in bytes
     * @return the weight in bytes
     */
    public int weight() {
        return numKeys * 4;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public long getHitCount() {
        return hitCount.sum();
    }

    public long getMissCount(){
        return missCount.sum();
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
