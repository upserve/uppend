package com.upserve.uppend;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class Partition {

    private final LongLookup lookups;
    private final Blobs blobs;


    public Partition(Path partitionPath, PagedFileMapper pagedFileMapper, LongLookup longLookup){
        this.lookups = longLookup;
        blobs = new Blobs(partitionPath, pagedFileMapper);
    }

    void append(String key, byte[] blob, BlockedLongs blocks){

    }

    Stream<byte[]> read(String key, BlockedLongs blocks){

        return Stream.empty();
    }
    //TODO add other read methods flushed, sequential

    Stream<Map.Entry<String, Stream<Byte[]>>> scan(BlockedLongs blocks){
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

}
