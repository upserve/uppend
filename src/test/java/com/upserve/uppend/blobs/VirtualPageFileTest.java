package com.upserve.uppend.blobs;

import org.junit.Test;

import java.nio.file.*;

public class VirtualPageFileTest {

    private String name = "virtual_page_file_test";
    private Path rootPath = Paths.get("build/test/blobStore");
    private Path path = rootPath.resolve(name);

    VirtualPageFile instance;

    @Test
    public void testReadWritePageAllocation(){

        instance = new VirtualPageFile(path, 36, 1024, false);

    }


}
