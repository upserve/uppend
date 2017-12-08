package com.upserve.uppend.lookup;

import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Iterator;

class LookupKeyIterator implements Iterator<LookupKey>, AutoCloseable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path path;
    private final Path keysPath;
    private final FileChannel chan;
    private final FileChannel keysChan;
    private final int numKeys;
    private int keyIndex = 0;

    LookupKeyIterator(Path path) throws IOException {
        this.path = path;
        chan = FileChannel.open(path, StandardOpenOption.READ);
        numKeys = (int) (chan.size() / 16);
        keysPath = path.resolveSibling("keys");
        keysChan = numKeys > 0 ? FileChannel.open(keysPath, StandardOpenOption.READ) : null;
    }

    int getNumKeys() {
        return numKeys;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < numKeys;
    }

    @Override
    public LookupKey next() {
        LookupKey key;
        try {
            key = LookupData.readKey(chan, keysChan, keyIndex);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read at key index " + keyIndex + " from " + path, e);
        }
        keyIndex++;
        return key;
    }

    @Override
    public void close() {
        try {
            chan.close();
        } catch (IOException e) {
            log.error("trouble closing: " + path, e);
        }
        try {
            keysChan.close();
        } catch (IOException e) {
            log.error("trouble closing: " + keysPath, e);
        }
    }
}
