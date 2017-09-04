package com.upserve.uppend.lookup;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;

@Slf4j
public class LookupMetadata {
    private final int keyLength;
    private final int numKeys;
    private final LookupKey minKey;
    private final LookupKey maxKey;
    private final int[] keyStorageOrder;

    public LookupMetadata(int keyLength, int numKeys, LookupKey minKey, LookupKey maxKey, int[] keyStorageOrder) {
        this.keyLength = keyLength;
        this.numKeys = numKeys;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStorageOrder = keyStorageOrder;
    }

    public LookupMetadata(Path path) {
        log.trace("constructing metadata at path: {}", path);
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ)) {
            try (DataInputStream din = new DataInputStream(Files.newInputStream(path, StandardOpenOption.READ))) {
                keyLength = din.readInt();
                numKeys = din.readInt();
                byte[] minKeyBytes = new byte[keyLength];
                din.read(minKeyBytes);
                minKey = new LookupKey(minKeyBytes);
                byte[] maxKeyBytes = new byte[keyLength];
                din.read(maxKeyBytes);
                maxKey = new LookupKey(maxKeyBytes);
            }
            long pos = 8 + 2 * keyLength;
            int mapSize = 4 * numKeys;
            MappedByteBuffer mbuf = chan.map(FileChannel.MapMode.READ_ONLY, pos, mapSize);
            IntBuffer ibuf = mbuf.asIntBuffer();
            keyStorageOrder = new int[numKeys];
            ibuf.get(keyStorageOrder);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to construct metadata from path: " + path, e);
        }
    }

    public long readData(Path dataPath, LookupKey key) {
        try {
            try (FileChannel dataChan = FileChannel.open(dataPath, StandardOpenOption.READ)) {
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
                        return LookupData.readValue(dataChan, keyLength, keyStorageOrder[keyIndexLower]);
                    }
                    comparison = upperKey.compareTo(key);
                    if (comparison < 0 /* upperKey is less than key */ ) {
                        return -1;
                    }
                    if (comparison == 0) {
                        return LookupData.readValue(dataChan, keyLength, keyStorageOrder[keyIndexUpper]);
                    }
                    int midpointPercentage = searchMidpointPercentage(lowerKey.string(), upperKey.string(), key.string());
                    int midpointKeyIndex = keyIndexLower + 1 + ((keyIndexUpper - keyIndexLower) * midpointPercentage / 100);
                    if (midpointKeyIndex >= keyIndexUpper) {
                        midpointKeyIndex = keyIndexUpper - 1;
                    }
                    log.trace("reading {} from {}: [{}, {}], [{}, {}], {}", key, dataPath, keyIndexLower, keyIndexUpper, lowerKey, upperKey, midpointKeyIndex);
                    int keyNumber = keyStorageOrder[midpointKeyIndex];
                    LookupKey midpointKey = LookupData.readKey(dataChan, keyLength, keyNumber);
                    comparison = key.compareTo(midpointKey);
                    if (comparison < 0) {
                        keyIndexUpper = midpointKeyIndex - 1;
                        upperKey = midpointKey;
                    } else if (comparison > 0) {
                        keyIndexLower = midpointKeyIndex + 1;
                        lowerKey = midpointKey;
                    } else {
                        return LookupData.readValue(dataChan, keyLength, midpointKeyIndex);
                    }
                } while (keyIndexLower < keyIndexUpper);
                return -1;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not read key " + key + " at " + dataPath, e);
        }
    }

    public void writeTo(Path path) throws IOException {
        log.trace("writing metadata to path: {}", path);
        Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel chan = FileChannel.open(tmpPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            try (DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(chan), 1024))) {
                if (keyLength != minKey.byteLength()) {
                    throw new IllegalStateException("expected key length (" + keyLength + ") does not agree with minKey length (" + minKey.byteLength() + ")");
                }
                if (keyLength != maxKey.byteLength() || keyLength != maxKey.byteLength()) {
                    throw new IllegalStateException("expected key length (" + keyLength + ") does not agree with maxKey length (" + maxKey.byteLength() + ")");
                }

                dout.writeInt(keyLength);
                dout.writeInt(numKeys);
                dout.write(minKey.bytes());
                dout.write(maxKey.bytes());

                long pos =  8 + 2 * keyLength;
                int mapSize = 4 * numKeys;
                MappedByteBuffer mbuf = chan.map(FileChannel.MapMode.READ_WRITE, pos, mapSize);
                IntBuffer ibuf = mbuf.asIntBuffer();
                ibuf.put(keyStorageOrder);
                mbuf.force();
            }
        }
        Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);
        log.trace("wrote metadata to path: {}: keyLength={}, numKeys={}, minKey={}, maxKey={}", path, keyLength, numKeys, minKey, maxKey);
    }

    @Override
    public String toString() {
        return "LookupMetadata{" +
                "keyLength=" + keyLength +
                ", numKeys=" + numKeys +
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
        return 100 * keyDistance / rangeDistance;
    }
}
