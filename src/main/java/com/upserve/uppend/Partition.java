package com.upserve.uppend;

import com.google.common.hash.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Math.min;

public abstract class Partition implements Flushable, Closeable, Trimmable {
    private static final int MAX_HASH_COUNT = 1 << 14; /* 16,384 */

    private static final int HASH_SEED = 219370429;

    final VirtualPageFile longKeyFile;
    final VirtualPageFile metadataBlobFile;

    private final HashFunction hashFunction;
    protected final boolean readOnly;
    final int hashCount;

    final LookupData[] lookups;

    Partition(VirtualPageFile longKeyFile, VirtualPageFile metadataBlobFile, boolean readOnly, FileStoreBuilder builder) {

        this.longKeyFile = longKeyFile;
        this.metadataBlobFile = metadataBlobFile;

        this.hashCount = builder.getLookupHashCount();
        this.readOnly = readOnly;

        if (hashCount < 1) {
            throw new IllegalArgumentException("hashCount must be >= 1");
        }
        if (hashCount > MAX_HASH_COUNT) {
            throw new IllegalArgumentException("hashCount must be <= " + MAX_HASH_COUNT);
        }

        if (hashCount == 1) {
            hashFunction = null;
        } else {
            hashFunction = Hashing.murmur3_32(HASH_SEED);
        }

        IntFunction<LookupData> constructorFunction = lookupDataFunction(readOnly, builder.getFlushThreshold(), builder.getMetadataTTL(), builder);

        lookups = IntStream.range(0, hashCount)
                .mapToObj(constructorFunction)
                .toArray(LookupData[]::new);
    }

    static Path validatePartition(Path parentDir, String partition) {
        validatePartition(partition);
        Path partitionDir = parentDir.resolve(partition);
        try {
            Files.createDirectories(partitionDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to make partition directory: " + partitionDir, e);
        }
        return partitionDir;
    }

    private IntFunction<LookupData> lookupDataFunction(boolean readOnly, int flushThreshold, int relaodInterval, FileStoreBuilder builder) {
        if (readOnly) {
            return virtualFileNumber -> LookupData.lookupReader(
                    new VirtualLongBlobStore(virtualFileNumber, longKeyFile, builder.getLongBlobStoreMetricsAdders()),
                    new VirtualMutableBlobStore(virtualFileNumber, metadataBlobFile, builder.getMutableBlobStoreMetricsAdders()),
                    relaodInterval,
                    builder.getLookupDataMetricsAdders()
            );
        } else {
            return virtualFileNumber -> LookupData.lookupWriter(
                    new VirtualLongBlobStore(virtualFileNumber, longKeyFile, builder.getLongBlobStoreMetricsAdders()),
                    new VirtualMutableBlobStore(virtualFileNumber, metadataBlobFile, builder.getMutableBlobStoreMetricsAdders()),
                    flushThreshold,
                    builder.getLookupDataMetricsAdders()
            );
        }
    }

    /**
     * A function for estimating an efficient buffer size for key and metadata files
     * Use the smaller of 2 pages for every hash or the target buffer size
     * @param pageSize the size of the page in bytes
     * @param hashCount the hash count of the partition
     * @param targetBufferSize The configured maximum target size for buffers
     * @return the adjusted buffer size to use for metadata or key data
     */
    static int adjustedTargetBufferSize(int pageSize, int hashCount, int targetBufferSize) {
        return (int) min((long) (pageSize + 16) * hashCount * 2, (long) targetBufferSize);
    }

    int keyHash(LookupKey key) {
        if (hashFunction == null){
            return 0;
        } else {
            return Math.abs(hashFunction.hashBytes(key.bytes()).asInt()) % hashCount;
        }
    }

    static Path metadataPath(Path partitionDir) {
        return partitionDir.resolve("keyMetadata");
    }

    static Path keysPath(Path partitionDir) {
        return partitionDir.resolve("keys");
    }

    static void validatePartition(String partition) {
        if (partition == null) {
            throw new NullPointerException("null partition");
        }
        if (partition.isEmpty()) {
            throw new IllegalArgumentException("empty partition");
        }

        if (!isValidPartitionCharStart(partition.charAt(0))) {
            throw new IllegalArgumentException("bad first-char of partition: " + partition);
        }

        for (int i = partition.length() - 1; i > 0; i--) {
            if (!isValidPartitionCharPart(partition.charAt(i))) {
                throw new IllegalArgumentException("bad char at position " + i + " of partition: " + partition);
            }
        }
    }

    @Override
    public void flush() {
        Arrays.stream(lookups).forEach(LookupData::flush);
    }

    @Override
    public void trim() {
        Arrays.stream(lookups).forEach(LookupData::trim);
    }

    @Override
    public void close() throws IOException {
        if (!readOnly) flush();

        longKeyFile.close();
        metadataBlobFile.close();
    }

    public VirtualPageFile getLongKeyFile() { return longKeyFile; }

    public VirtualPageFile getMetadataBlobFile() {return metadataBlobFile; }

    private static boolean isValidPartitionCharStart(char c) {
        return Character.isJavaIdentifierPart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }

    public long keyCount() {
        return Arrays.stream(lookups).mapToLong(LookupData::keyCount).sum();
    }
}
