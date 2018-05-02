package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.util.concurrent.ConcurrentHashMap;

public class LookupMetadata {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MAX_BISECT_KEY_CACHE_DEPTH = 9;

    private final int metadataGeneration;

    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final long[] keyStorageOrder;

    private final ConcurrentHashMap<Long, LookupKey> bisectKeys;

    public static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, long[] keyStorageOrder, VirtualMutableBlobStore metaDataBlobs, int metadataGeneration) throws IOException {

        LookupMetadata newMetadata = new LookupMetadata(
                minKey,
                maxKey,
                keyStorageOrder,
                metadataGeneration
        );

        newMetadata.writeTo(metaDataBlobs);

        return newMetadata;
    }

    LookupMetadata(LookupKey minKey, LookupKey maxKey, long[] keyStorageOrder, int metadataGeneration) {
        this.numKeys = keyStorageOrder.length;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
        this.metadataGeneration = metadataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
    }

    public static LookupMetadata open(VirtualMutableBlobStore metadataBlobs, int metadataGeneration){
        if (metadataBlobs.isPageAllocated(0L)) {
            byte[] bytes = metadataBlobs.read(0L);
            return new LookupMetadata(bytes, metadataGeneration);
        } else {
            return new LookupMetadata(null, null, new long[0], metadataGeneration);
        }
    }

    LookupMetadata(byte[] bytes, int metadataGeneration) {
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

            LongBuffer lbuf = buffer.asLongBuffer();
            keyStorageOrder = new long[numKeys];
            lbuf.get(keyStorageOrder);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Meta blob is corrupted", e); // The checksum is correct - indicates a format change!
        }

        this.metadataGeneration = metadataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
        }

    /**
     * Finds the value associated with a key or null if not present using bisect on the sorted storage order
     * If the result is null (key not found) the key is marked with the generation of the metadata used and the
     * sortIndex it should be inserted after.
     * If the result is not null (key was found) the key is marked with its position in the longBlob file.
     * @param longBlobStore The longBlobStore to read keys and values
     * @param key the key to find and mark
     * @return the position of the key
     */

    public Long findKey(VirtualLongBlobStore longBlobStore, LookupKey key) {

        key.setMetaDataGeneration(metadataGeneration);

        if (numKeys == 0){
            key.setInsertAfterSortIndex(-1);
            return null;
        }

        int keyIndexLower = 0;
        int keyIndexUpper = numKeys - 1;
        LookupKey lowerKey = minKey;
        LookupKey upperKey = maxKey;

        int bisectCount = 0;
        long keyPosition;
        LookupKey midpointKey;
        int midpointKeyIndex;

        int comparison = lowerKey.compareTo(key);
        if (comparison > 0 /* new key is less than lowerKey */) {
            key.setInsertAfterSortIndex(-1); // Insert it after this index in the sort order
            return null;
        }
        if (comparison == 0) {
            key.setPosition(keyStorageOrder[keyIndexLower]);
            return longBlobStore.readLong(keyStorageOrder[keyIndexLower]);
        }

        comparison = upperKey.compareTo(key);
        if (comparison < 0 /* new key is greater than upperKey */) {
            key.setInsertAfterSortIndex(keyIndexUpper); // Insert it after this index in the sort order
            return null;
        }
        if (comparison == 0) {
            key.setPosition(keyStorageOrder[keyIndexUpper]);
            return longBlobStore.readLong(keyStorageOrder[keyIndexUpper]);
        }

        if (numKeys == 2) { // There are no other values keys besides upper and lower
            key.setInsertAfterSortIndex(keyIndexLower);
            return null;
        }

        // bisect till we find the key or return null
        do {
            midpointKeyIndex = keyIndexLower + ((keyIndexUpper - keyIndexLower) / 2);

            if (log.isTraceEnabled()) log.trace("reading {}: [{}, {}], [{}, {}], {}", key, keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);

            keyPosition = keyStorageOrder[midpointKeyIndex];
            // Cache only the most frequently used midpoint keys
            if (bisectCount < MAX_BISECT_KEY_CACHE_DEPTH) {
                midpointKey = bisectKeys.computeIfAbsent(keyPosition, position -> new LookupKey(longBlobStore.readBlob(position)));
            } else {
                midpointKey = new LookupKey(longBlobStore.readBlob(keyPosition));
            }

            comparison = key.compareTo(midpointKey);
            if (comparison < 0) {
                upperKey = midpointKey;
                keyIndexUpper = midpointKeyIndex;
            } else if (comparison > 0) {
                keyIndexLower = midpointKeyIndex;
                lowerKey = midpointKey;
            } else {
                key.setPosition(keyPosition);
                return longBlobStore.readLong(keyPosition);
            }

            bisectCount++;
        } while ((keyIndexLower +1) < keyIndexUpper);

        key.setInsertAfterSortIndex(keyIndexLower); // Insert it in the sort order after this key
        return null;
    }

    public void writeTo(VirtualMutableBlobStore metadataBlobs) {
        int headerSize = 12 + minKey.byteLength() + maxKey.byteLength();
        int longBufSize = 8 * numKeys;
        ByteBuffer byteBuffer = ByteBuffer.allocate(headerSize + longBufSize);
        byteBuffer.putInt(numKeys);
        byteBuffer.putInt(minKey.byteLength());
        byteBuffer.put(minKey.bytes());
        byteBuffer.putInt(maxKey.byteLength());
        byteBuffer.put(maxKey.bytes());

        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(keyStorageOrder);
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

    public int getMetadataGeneration(){
        return metadataGeneration;
    }

    public int weight() {
        return numKeys;
    }

    public int getNumKeys(){
        return numKeys;
    }

    public long[] getKeyStorageOrder(){
        return keyStorageOrder;
    }

    public LookupKey getMinKey(){
        return minKey;
    }

    public LookupKey getMaxKey(){
        return maxKey;
    }
}
