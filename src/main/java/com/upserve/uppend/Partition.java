package com.upserve.uppend;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class Partition {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path partitiondDir;
    private final LongLookup lookups;
    private final Blobs blobs;

    public static Partition createPartition(Path partentDir, String partition, int hashSize, PagedFileMapper blobPageCache, LookupCache lookupCache){
        Partition.validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        return new Partition(
                partitiondDir,
                new LongLookup(partitiondDir.resolve("lookups"), hashSize, PartitionLookupCache.create(partition, lookupCache)),
                new Blobs(partitiondDir.resolve("blobs"), blobPageCache)
                );
    }


    private Partition(Path partentDir, LongLookup longLookup, Blobs blobs){
        partitiondDir = partentDir;
        this.lookups = longLookup;
        this.blobs = blobs;
    }

    void append(String key, byte[] blob, BlockedLongs blocks){
        long blobPos = blobs.append(blob);
        long blockPos = lookups.putIfNotExists(key, blocks::allocate);
        blocks.append(blockPos, blobPos);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for path '{}', key '{}'", blob.length, blobPos, blockPos, partitiondDir, key);
    }

    Stream<byte[]> read(String key, BlockedLongs blocks){
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
        return blocks.values(lookups.get(key)).mapToObj(blobs::read);
    }

    //TODO add other read methods flushed, sequential

    Stream<Map.Entry<String, Stream<Byte[]>>> scan(BlockedLongs blocks){

        lookups.scan();


        return Stream.empty();
    }


    void scan(BlockedLongs blocks, BiConsumer<String, Stream<byte[]>> callback){

    }

    void flush(){

    }

    void close(){

    }

    void trim(){

    }

    void clear(){

    }

    public static void validatePartition(String partition) {
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

    private static boolean isValidPartitionCharStart(char c) {
        return Character.isJavaIdentifierPart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }
}
