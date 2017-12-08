package com.upserve.uppend.lookup;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class KeyIterator implements Iterator<LookupKey>, Closeable {
    private final Path path;
    private final FileChannel chan;
    private final FileChannel keysChan;
    private final int numKeys;
    private int keyIndex = 0;

    public KeyIterator(Path path) throws IOException {
        this.path = path;
        chan = FileChannel.open(path, StandardOpenOption.READ);
        numKeys = (int) (chan.size() / 16);
        keysChan = numKeys > 0 ? FileChannel.open(path.resolveSibling("keys"), StandardOpenOption.READ) : null;
    }

    public int getNumKeys() {
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
    public void close() throws IOException {
        if (chan != null && chan.isOpen()) chan.close();
        if (keysChan != null && keysChan.isOpen()) keysChan.close();
    }
}