package com.upserve.uppend;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@Slf4j
public class FileAppendOnlyStore implements AppendOnlyStore {
    private static final int NUM_BLOBS_PER_BLOCK = 127;
    private static final int MAX_LOOKUPS_CACHE_SIZE = 4096;

    private final HashedLongLookups lookups;
    private final BlockedLongs blocks;
    private final Blobs blobs;

    public FileAppendOnlyStore(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        lookups = new HashedLongLookups(dir.resolve("lookups"), MAX_LOOKUPS_CACHE_SIZE);
        blocks = new BlockedLongs(dir.resolve("blocks"), NUM_BLOBS_PER_BLOCK);
        blobs = new Blobs(dir.resolve("blobs"));
    }

    @Override
    public void append(String key, byte[] value) {
        long blobPos = blobs.append(value);
        LongLookup lookup = lookups.get(key);
        long blockPos = lookup.putIfNotExists(key, blocks::allocate);
        log.trace("appending {} bytes (blob pos {}) for key '{}' at block pos {}", value.length, blobPos, key, blockPos);
        blocks.append(blockPos, blobPos);
    }

    @Override
    public void read(String key, Consumer<byte[]> reader) {
        LongStream blockValues = blockValues(key);
        if (blockValues == null) {
            return;
        }
        blockValues
                .parallel()
                .forEach(pos -> reader.accept(blobs.read(pos)));
    }

    @Override
    public Stream<byte[]> read(String key) {
        LongStream blockValues = blockValues(key);
        if (blockValues == null) {
            return null;
        }
        return blockValues
                .parallel()
                .mapToObj(blobs::read);
    }

    @Override
    public void clear() {
        log.trace("clearing");
        blocks.clear();
        blobs.clear();
        lookups.clear();
    }

    @Override
    public void close() throws Exception {
        log.info("closing");
        try {
            blocks.close();
        } catch (Exception e) {
            log.error("unable to close blocks", e);
        }
        try {
            blobs.close();
        } catch (Exception e) {
            log.error("unable to close blobs", e);
        }
        try {
            lookups.close();
        } catch (Exception e) {
            log.error("unable to close lookups", e);
        }
    }

    private LongStream blockValues(String key) {
        log.trace("reading key: {}", key);
        LongLookup lookup = lookups.get(key);
        Long blockPos = lookup.get(key);
        if (blockPos == null) {
            return null;
        }
        log.trace("streaming values at block pos {} for key: {}", blockPos, key);
        return blocks.values(blockPos);
    }
}
