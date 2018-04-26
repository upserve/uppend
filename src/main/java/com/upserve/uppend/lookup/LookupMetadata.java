package com.upserve.uppend.lookup;

import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
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

    public static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, long[] keyStorageOrder, Path path, int metadataGeneration) throws IOException {

        LookupMetadata newMetadata = new LookupMetadata(
                minKey,
                maxKey,
                keyStorageOrder,
                metadataGeneration
        );

        newMetadata.writeTo(path);

        return newMetadata;
    }


    public LookupMetadata(LookupKey minKey, LookupKey maxKey, long[] keyStorageOrder, int metadataGeneration) {
        this.numKeys = keyStorageOrder.length;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
        this.metadataGeneration = metadataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
    }

    public static LookupMetadata open(Path path, int metadataGeneration){
        try {
            return new LookupMetadata(path, metadataGeneration);
        } catch (NoSuchFileException e) {
            log.debug("No metadata found at path {}", path);
            return new LookupMetadata(null, null, new long[0], metadataGeneration);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to load metadata from path:" + path, e);
        }
    }

    public LookupMetadata(Path path, int metadataGeneration) throws IOException {
        log.trace("constructing metadata at path: {}", path);
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
            int minKeyLength, maxKeyLength;
            try (DataInputStream din = new DataInputStream(Files.newInputStream(path, StandardOpenOption.READ))) {
                numKeys = din.readInt();
                minKeyLength = din.readInt();
                byte[] minKeyBytes = new byte[minKeyLength];
                din.read(minKeyBytes); // should check result - number of bytes read
                minKey = new LookupKey(minKeyBytes);
                maxKeyLength = din.readInt();
                byte[] maxKeyBytes = new byte[maxKeyLength];
                din.read(maxKeyBytes);
                maxKey = new LookupKey(maxKeyBytes);
            }
            long pos = 12 + minKeyLength + maxKeyLength;
            int byteSize = 8 * numKeys;
            ByteBuffer bbuf = ByteBuffer.allocate(byteSize);
            int bytesRead = chan.read(bbuf, pos);
            if (bytesRead != byteSize) throw new IllegalStateException("expected compressed key storage order size " + byteSize + " but got " + bytesRead);
            bbuf.rewind();
            LongBuffer lbuf = bbuf.asLongBuffer();
            keyStorageOrder = new long[numKeys];
            lbuf.get(keyStorageOrder);
        }

        this.metadataGeneration = metadataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
        }

    /**
     * Finds the position of a key or null if not present using bisect on the sorted storage order
     * The key is marked with the generation of the metadata used and the sortIndex it should be inserted after
     * @param lookupData a reference to use for reading keys
     * @param key the key to find and mark
     * @return the position of the key
     */

    public Long findKeyPosition(LookupData lookupData, LookupKey key) {

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
            return keyStorageOrder[keyIndexLower];
        }

        comparison = upperKey.compareTo(key);
        if (comparison < 0 /* new key is greater than upperKey */) {
            key.setInsertAfterSortIndex(keyIndexUpper); // Insert it after this index in the sort order
            return null;
        }
        if (comparison == 0) {
            return keyStorageOrder[keyIndexUpper];
        }

        if (numKeys == 2) { // There are no other values keys besides upper and lower
            key.setInsertAfterSortIndex(keyIndexLower);
            return null;
        }

        // bisect till we find the key or return null
        do {
            midpointKeyIndex = keyIndexLower + ((keyIndexUpper - keyIndexLower) / 2);

            if (log.isTraceEnabled()) log.trace("reading {} from {}: [{}, {}], [{}, {}], {}", key, lookupData.getHashPath(), keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);

            keyPosition = keyStorageOrder[midpointKeyIndex];
            // Cache only the most frequently used midpoint keys
            if (bisectCount < MAX_BISECT_KEY_CACHE_DEPTH) {
                midpointKey = bisectKeys.computeIfAbsent(keyPosition, lookupData::readKey);
            } else {
                midpointKey = lookupData.readKey(keyPosition);
            }

            comparison = key.compareTo(midpointKey);
            if (comparison < 0) {
                upperKey = midpointKey;
                keyIndexUpper = midpointKeyIndex;
            } else if (comparison > 0) {
                keyIndexLower = midpointKeyIndex;
                lowerKey = midpointKey;
            } else {
                return keyPosition;
            }

            bisectCount++;
        } while ((keyIndexLower +1) < keyIndexUpper);

        key.setInsertAfterSortIndex(keyIndexLower); // Insert it in the sort order after this key
        return null;
    }

    public void writeTo(Path path) throws IOException {
        log.trace("writing metadata to path: {}", path);
        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel chan = FileChannel.open(tmpPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            int bufSize = 12 + minKey.byteLength() + maxKey.byteLength();
            ByteBuffer headBuf = ByteBuffer.allocate(bufSize);
            headBuf.putInt(numKeys);
            headBuf.putInt(minKey.byteLength());
            headBuf.put(minKey.bytes());
            headBuf.putInt(maxKey.byteLength());
            headBuf.put(maxKey.bytes());
            headBuf.flip();
            chan.write(headBuf, 0);

            int longBufSize = 8 * numKeys;
            ByteBuffer byteBuffer = ByteBuffer.allocate(longBufSize);
            LongBuffer longBuffer = byteBuffer.asLongBuffer();
            longBuffer.put(keyStorageOrder);
            byteBuffer.rewind();
            int written = chan.write(byteBuffer, bufSize);
            if (written != longBufSize) {
                throw new RuntimeException("Expected " + bufSize + " but wrote" + written +" bytes to metadata " + path);
            }

        }
        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);
        log.trace("wrote metadata to path: {}: numKeys={}, minKey={}, maxKey={}", path, numKeys, minKey, maxKey);
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
