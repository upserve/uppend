package com.upserve.uppend;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.stream.Stream;

public abstract class Partition {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static Path lookupsDir(Path partitiondDir){
        return partitiondDir.resolve("lookups");
    }

    public static Stream<String> listPartitions(Path partitiondPath){
        try {
            return Files.list(partitiondPath).filter(path -> Files.exists(lookupsDir(path))).map(path -> path.toFile().getName());
        } catch (NoSuchFileException e){
            log.debug("Partitions director does not exist: {}", partitiondPath);
            return Stream.empty();

        } catch (IOException e){
            log.error("Unable to list partitions in {}", partitiondPath, e);
            return Stream.empty();
        }
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

    private static boolean isValidPartitionCharStart(char c) {
        return Character.isJavaIdentifierPart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }
}
