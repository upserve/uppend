package com.upserve.uppend.lookup;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public class LookupMetadata {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final int[] keyStorageOrder;

    public LookupMetadata(int numKeys, LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder) {
        this.numKeys = numKeys;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
    }

    public LookupMetadata(Path path) {
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
    }

    public long readData(Path dataPath, LookupKey key) {
        try {
            try (FileChannel dataChan = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                if (numKeys > LookupData.numEntries(dataChan)) {
                    throw new IOException("metadata num keys (" + numKeys + ") exceeds num data entries");
                }

                Path keysPath = dataPath.resolveSibling("keys");
                try (FileChannel keysChan = FileChannel.open(keysPath, StandardOpenOption.READ)) {
                    int keyIndexLower = 0;
                    int keyIndexUpper = numKeys - 1;
                    LookupKey lowerKey = minKey;
                    LookupKey upperKey = maxKey;
                    do {
                        int comparison = lowerKey.compareTo(key);
                        if (comparison > 0 /* lowerKey is greater than key */) {
                            return -1;
                        }
                        if (comparison == 0) {
                            return LookupData.readValue(dataChan, keyStorageOrder[keyIndexLower]);
                        }
                        comparison = upperKey.compareTo(key);
                        if (comparison < 0 /* upperKey is less than key */) {
                            return -1;
                        }
                        if (comparison == 0) {
                            return LookupData.readValue(dataChan, keyStorageOrder[keyIndexUpper]);
                        }
                        int midpointPercentage = searchMidpointPercentage(lowerKey.string(), upperKey.string(), key.string());
                        int midpointKeyIndex = keyIndexLower + 1 + ((keyIndexUpper - keyIndexLower) * midpointPercentage / 100);
                        if (midpointKeyIndex >= keyIndexUpper) {
                            midpointKeyIndex = keyIndexUpper - 1;
                        }
                        log.trace("reading {} from {}: [{}, {}], [{}, {}], {}", key, dataPath, keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);
                        int keyNumber = keyStorageOrder[midpointKeyIndex];
                        LookupKey midpointKey = LookupData.readKey(dataChan, keysChan, keyNumber);
                        comparison = key.compareTo(midpointKey);
                        if (comparison < 0) {
                            keyIndexUpper = midpointKeyIndex - 1;
                            keyIndexLower++;
                        } else if (comparison > 0) {
                            keyIndexLower = midpointKeyIndex + 1;
                            keyIndexUpper--;
                        } else {
                            return LookupData.readValue(dataChan, keyNumber);
                        }
                        upperKey = LookupData.readKey(dataChan, keysChan, keyStorageOrder[keyIndexUpper]);
                        lowerKey = LookupData.readKey(dataChan, keysChan, keyStorageOrder[keyIndexLower]);
                    } while (keyIndexLower <= keyIndexUpper);
                    return -1;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not read key " + key + " at " + dataPath, e);
        }
    }

    public void writeTo(Path path) throws IOException {
        log.trace("writing metadata to path: {}", path);
        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel chan = FileChannel.open(tmpPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            IntegratedIntCompressor iic = new IntegratedIntCompressor();
            int[] compressedOrdering = iic.compress(keyStorageOrder);
            int compressedSize = compressedOrdering.length;

            byte[] minKeyBytes = minKey.bytes();
            byte[] maxKeyBytes = maxKey.bytes();

            int bufSize = 16 + minKeyBytes.length + maxKeyBytes.length;
            ByteBuffer headBuf = ByteBuffer.allocate(bufSize);
            headBuf.putInt(numKeys);
            headBuf.putInt(compressedSize);

            headBuf.putInt(minKeyBytes.length);
            headBuf.put(minKeyBytes);

            headBuf.putInt(maxKeyBytes.length);
            headBuf.put(maxKeyBytes);
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
}
