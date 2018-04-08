package com.upserve.uppend.lookup;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;

public class LookupMetadata {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MAX_BISECT_KEY_CACHE_DEPTH = 6;

    private final int metadataGeneration;

    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final int[] keyStorageOrder;

    private final ConcurrentHashMap<Integer, LookupKey> bisectKeys;

    public static LookupMetadata generateMetadata(LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, Path path, int metadataGeneration) throws IOException {

        LookupMetadata newMetadata = new LookupMetadata(
                keyStorageOrder.length,
                minKey,
                maxKey,
                keyStorageOrder,
                metadataGeneration
        );

        newMetadata.writeTo(path);

        return newMetadata;
    }


    public LookupMetadata(int numKeys, LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metadataGeneration) {
        this.numKeys = numKeys;
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
            return new LookupMetadata(0, null, null, new int[0], metadataGeneration);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to load metadata from path:" + path, e);
        }
    }

    public LookupMetadata(Path path, int metadataGeneration) throws IOException {
        log.trace("constructing metadata at path: {}", path);
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
            int compressedSize, minKeyLength, maxKeyLength;
            try (DataInputStream din = new DataInputStream(Files.newInputStream(path, StandardOpenOption.READ))) {
                numKeys = din.readInt();
                compressedSize = din.readInt();
                minKeyLength = din.readInt();
                byte[] minKeyBytes = new byte[minKeyLength];
                din.read(minKeyBytes);
                minKey = new LookupKey(minKeyBytes);
                maxKeyLength = din.readInt();
                byte[] maxKeyBytes = new byte[maxKeyLength];
                din.read(maxKeyBytes);
                maxKey = new LookupKey(maxKeyBytes);
            }
            long pos = 16 + minKeyLength + maxKeyLength;
            int mapSize = 4 * compressedSize;
            ByteBuffer bbuf = ByteBuffer.allocate(mapSize);
            chan.read(bbuf, pos);
            bbuf.rewind();
            IntBuffer ibuf = bbuf.asIntBuffer();
            int[] compressedOrdering = new int[compressedSize];
            ibuf.get(compressedOrdering);
            IntegratedIntCompressor iic = new IntegratedIntCompressor();
            keyStorageOrder = iic.uncompress(compressedOrdering);
            if (keyStorageOrder.length != numKeys) {
                throw new IllegalStateException("expected " + numKeys + " keys, got " + keyStorageOrder.length + " at path: " + path);
            }
        }

        this.metadataGeneration = metadataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
        }

    /**
     * Finds the long value associated with a key or null if not present using bisect on the sorted storage order
     * The key is marked with the generation of the metadata used and the keyindex of append only block that contains the value
     * @param lookupData a reference to use for reading blocks and keys
     * @param key the key to find and mark
     * @return the long value associated with that key
     */

    public Long findLong(LookupData lookupData, LookupKey key) {

        key.setMetaDataGeneration(metadataGeneration);

        if (numKeys == 0){
            key.setLookupBlockIndex(-1);
            return null;
        }

        int keyIndexLower = 0;
        int keyIndexUpper = numKeys - 1;
        LookupKey lowerKey = minKey;
        LookupKey upperKey = maxKey;

        int bisectCount = 0;
        int keyNumber;
        LookupKey midpointKey;
        int midpointKeyIndex;


        int comparison = lowerKey.compareTo(key);
        if (comparison > 0 /* lowerKey is greater than key */) {
            key.setLookupBlockIndex(-1);
            return null;
        }
        if (comparison == 0) {
            keyNumber = keyStorageOrder[keyIndexLower];
            key.setLookupBlockIndex(keyNumber); // This is the key index in the data file
            return lookupData.getKeyValue(keyNumber);
        }
        comparison = upperKey.compareTo(key);
        if (comparison < 0 /* upperKey is less than key */) {
            key.setLookupBlockIndex(keyStorageOrder[keyIndexUpper]); // Insert it after this index in the sort order
            return null;
        }
        if (comparison == 0) {
            keyNumber = keyStorageOrder[keyIndexUpper];
            key.setLookupBlockIndex(keyNumber); // This is the key index in the data file
            return lookupData.getKeyValue(keyNumber);
        }

        // bisect till we find the key or return null
        do {
            midpointKeyIndex = keyIndexLower + ((keyIndexUpper - keyIndexLower) / 2);

            if (log.isTraceEnabled()) log.trace("reading {} from {}: [{}, {}], [{}, {}], {}", key, lookupData.getHashPath(), keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);

            keyNumber = keyStorageOrder[midpointKeyIndex];
            // Cache only the most frequently used midpoint keys
            if (bisectCount < MAX_BISECT_KEY_CACHE_DEPTH) {
                midpointKey = bisectKeys.computeIfAbsent(keyNumber, lookupData::readKey);
            } else {
                midpointKey = lookupData.readKey(keyNumber);
            }

            comparison = key.compareTo(midpointKey);
            if (comparison < 0) {
                upperKey = midpointKey;
                keyIndexUpper = midpointKeyIndex;
            } else if (comparison > 0) {
                keyIndexLower = midpointKeyIndex;
                lowerKey = midpointKey;
            } else {
                key.setLookupBlockIndex(keyNumber); // This is the index in the data file
                return lookupData.getKeyValue(keyNumber);
            }

            bisectCount++;
        } while ((keyIndexLower +1) < keyIndexUpper);
        key.setMetaDataGeneration(metadataGeneration);
        key.setLookupBlockIndex(keyStorageOrder[keyIndexLower]); // Insert it in the sort order after this key
        return null;
    }

    public void writeTo(Path path) throws IOException {
        log.trace("writing metadata to path: {}", path);
        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel chan = FileChannel.open(tmpPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            IntegratedIntCompressor iic = new IntegratedIntCompressor();
            int[] compressedOrdering = iic.compress(keyStorageOrder);
            int compressedSize = compressedOrdering.length;

            int bufSize = 16 + minKey.byteLength() + maxKey.byteLength();
            ByteBuffer headBuf = ByteBuffer.allocate(bufSize);
            headBuf.putInt(numKeys);
            headBuf.putInt(compressedSize);
            headBuf.putInt(minKey.byteLength());
            headBuf.put(minKey.bytes());
            headBuf.putInt(maxKey.byteLength());
            headBuf.put(maxKey.bytes());
            headBuf.flip();
            chan.write(headBuf, 0);

            int mapSize = 4 * compressedSize;
            ByteBuffer bbuf = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(mapSize).get();
            IntBuffer ibuf = bbuf.asIntBuffer();
            ibuf.put(compressedOrdering);
            bbuf.rewind();
            chan.write(bbuf, bufSize);
            log.trace("compressed metadata: {}/{}", compressedSize, numKeys);
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

    public int[] getKeyStorageOrder(){
        return keyStorageOrder;
    }

    public LookupKey getMinKey(){
        return minKey;
    }

    public LookupKey getMaxKey(){
        return maxKey;
    }
}
