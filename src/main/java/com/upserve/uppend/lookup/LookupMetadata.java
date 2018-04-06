package com.upserve.uppend.lookup;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.ThreadLocalByteBuffers;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class LookupMetadata {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MAX_BISECT_KEY_CACHE_DEPTH = 6;

    private final int metaDataGeneration;

    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final int[] keyStorageOrder;

    private final ConcurrentHashMap<Integer, LookupKey> bisectKeys;

    public LookupMetadata(int numKeys, LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder, int metaDataGeneration) {
        this.numKeys = numKeys;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
        this.metaDataGeneration = metaDataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
    }

    public LookupMetadata(Path path, int metaDataGeneration) {
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
        } catch (IOException e) {
            throw new UncheckedIOException("unable to construct metadata from path: " + path, e);
        }

        this.metaDataGeneration = metaDataGeneration;

        bisectKeys = new ConcurrentHashMap<>();
        }

    public Long readData(LookupData lookupData, LookupKey key) {

        int keyIndexLower = 0;
        int keyIndexUpper = numKeys - 1;
        LookupKey lowerKey = minKey;
        LookupKey upperKey = maxKey;

        int bisectCount = 0;

        int comparison = lowerKey.compareTo(key);
        if (comparison > 0 /* lowerKey is greater than key */) {
            key.setLookupBlockIndex(keyIndexLower);
            key.setMetaDataGeneration(metaDataGeneration);
            return null;
        }
        if (comparison == 0) {
            return lookupData.getKeyValue(keyStorageOrder[keyIndexLower]);
        }
        comparison = upperKey.compareTo(key);
        if (comparison < 0 /* upperKey is less than key */) {
            key.setLookupBlockIndex(keyIndexUpper+1);
            key.setMetaDataGeneration(metaDataGeneration);
            return null;
        }
        if (comparison == 0) {
            return lookupData.getKeyValue(keyStorageOrder[keyIndexUpper]);
        }

        LookupKey midpointKey;
        int midpointKeyIndex;
        do {
            midpointKeyIndex = keyIndexLower + ((keyIndexUpper - keyIndexLower) / 2);

            if (log.isTraceEnabled()) log.trace("reading {} from {}: [{}, {}], [{}, {}], {}", key, lookupData.getHashPath(), keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);

            int keyNumber = keyStorageOrder[midpointKeyIndex];
            if (bisectCount < MAX_BISECT_KEY_CACHE_DEPTH) {
                midpointKey = bisectKeys.computeIfAbsent(keyNumber, lookupData::getKey);
            } else {
                midpointKey = lookupData.getKey(keyNumber);
            }

            comparison = key.compareTo(midpointKey);
            if (comparison < 0) {
                upperKey = midpointKey;
                keyIndexUpper = midpointKeyIndex;
            } else if (comparison > 0) {
                keyIndexLower = midpointKeyIndex;
                lowerKey = midpointKey;
            } else {
                return lookupData.getKeyValue(keyNumber);
            }

            bisectCount++;
        } while ((keyIndexLower +1) < keyIndexUpper);
        key.setMetaDataGeneration(metaDataGeneration);
        key.setLookupBlockIndex(keyIndexLower);
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

    static int searchMidpointPercentage(String lower, String upper, String key) {
        int firstDifferentCharIndex = -1;
        int minLength = Math.min(key.length(), Math.min(lower.length(), upper.length()));
        char lowerChar = 0, upperChar = 0, keyChar = 0;
        for (int i = 0; i < minLength; i++) {
            lowerChar = lower.charAt(i);
            upperChar = upper.charAt(i);
            keyChar = key.charAt(i);
            if (lowerChar != upperChar) {
                firstDifferentCharIndex = i;
                break;
            }
            if (keyChar != upperChar) {
                log.warn("returning 50% search midpoint for key ({}); key is outside of range [{}, {}]", key, lower, upper);
                return 50;
            }
        }
        if (firstDifferentCharIndex == -1) {
            log.trace("returning 50% search midpoint for key ({}) in single element range [{}, {}]", key, lower, upper);
            return 50;
        }
        int keyDistance = keyChar - lowerChar;
        int rangeDistance = upperChar - lowerChar;
        if (keyDistance < 0 || rangeDistance < 0 || keyDistance > rangeDistance) {
            log.warn("returning 50% search midpoint for key ({}); weight could not be determined from range [{}, {}]", key, lower, upper);
            return 50;
        }
        int pct = 100 * keyDistance / rangeDistance;
        if (pct > 90) {
            return 90;
        }
        if (pct < 10) {
            return 10;
        }
        return pct;
    }

    public int weight() {
        return numKeys;
    }
}
