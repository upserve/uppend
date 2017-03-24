package com.upserve.uppend;

import com.upserve.uppend.util.Varint;
import it.unimi.dsi.fastutil.objects.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.UnexpectedException;
import java.util.function.LongSupplier;

@Slf4j
public class LongLookup implements AutoCloseable {
    private static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;

    private final Path path;
    private final FileChannel chan;
    private final DataOutputStream out;
    private final Object2LongMap<String> mem;

    public LongLookup(Path path) {
        this(path, DEFAULT_FLUSH_DELAY_SECONDS);
    }

    public LongLookup(Path path, int flushDelaySeconds) {
        if (path == null) {
            throw new NullPointerException("null path");
        }
        this.path = path;
        Path parentPath = path.getParent();
        if (Files.notExists(parentPath)) {
            try {
                Files.createDirectories(parentPath);
            } catch (IOException e) {
                throw new UncheckedIOException("unable to make parent dir: " + parentPath, e);
            }
        }
        try {
            chan = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            chan.position(chan.size());
        } catch (IOException e) {
            throw new UncheckedIOException("can't open TOC file: " + path, e);
        }
        out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(chan), 8192));
        AutoFlusher.register(flushDelaySeconds, out);
        try {
            mem = scan(chan);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to scan " + path, e);
        }
    }

    public synchronized Long get(String key) {
        log.trace("getting from {}: {}", path, key);
        long v = mem.getLong(key);
        if (v == Long.MIN_VALUE) {
            return null;
        }
        return v;
    }

    public ObjectSet<String> keys() {
        return mem.keySet();
    }

    public synchronized void put(String key, long value) {
        log.trace("putting {}={} in {}", key, value, path);
        long existingValue = mem.put(key, value);
        if (existingValue != Long.MIN_VALUE) {
            throw new IllegalStateException("can't put same key ('" + key + "') twice: new value = " + value + ", existing value = " + existingValue);
        }
        append(key, value);
    }

    public synchronized long putIfNotExists(String key, long value) {
        log.trace("putting (if not exists) {}={} in {}", key, value, path);
        long existingValue = mem.put(key, value);
        if (existingValue != Long.MIN_VALUE) {
            return existingValue;
        }
        append(key, value);
        return value;
    }

    public synchronized long putIfNotExists(String key, LongSupplier allocateLongFunc) {
        log.trace("putting (if not exists) {}=<lambda> in {}", key, path);
        Long firstExistingValue = get(key);
        if (firstExistingValue == null) {
            long newValue = allocateLongFunc.getAsLong();
            long existingValue = putIfNotExists(key, newValue);
            if (existingValue != newValue) {
                log.warn("lost race to allocate, wasted new value " + newValue + " for key: " + key);
            }
            return existingValue;
        }
        return firstExistingValue;

    }

    @Override
    public synchronized void close() throws IOException {
        log.trace("closing {}", path);
        AutoFlusher.deregister(out);
        out.close();
        chan.close();
    }

    private void append(String key, long value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length > 255) {
            throw new IllegalStateException("key length exceeds 255 bytes: " + keyBytes.length);
        }
        try {
            out.write(keyBytes.length);
            out.write(keyBytes);
            Varint.write(out, value);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write key: " + key, e);
        }
    }

    private static Object2LongMap<String> scan(FileChannel chan) throws IOException {
        chan.position(0);
        long pos = 0;
        long size = chan.size();
        Object2LongMap<String> map = new Object2LongOpenHashMap<>();
        map.defaultReturnValue(Long.MIN_VALUE);
        DataInputStream dis = new DataInputStream(new BufferedInputStream(Channels.newInputStream(chan), 8192));
        int len;
        byte[] strBuf = new byte[256];
        while (pos < size && (len = dis.read()) != -1) {
            if (len == 0) {
                throw new UnexpectedException("unexpected: read 0 len at pos " + pos);
            }
            if (len > strBuf.length) {
                throw new UnexpectedException("unexpected: read too-large len (" + len + " > " + strBuf.length + ") at pos " + pos);
            }
            pos += 1;
            if (pos == size) {
                // corrupt; fix
                chan.truncate(size - 1);
                break;
            }
            try {
                dis.readFully(strBuf, 0, len);
            } catch (EOFException e) {
                throw new IOException("got eof at pos " + pos + " while trying to read " + len + " bytes", e);
            }
            pos += len;
            String key = new String(strBuf, 0, len, StandardCharsets.UTF_8);
            long val;
            try {
                val = Varint.readLong(dis);
            } catch (IOException e) {
                throw new IOException("read bad value for key '" + key + "' at pos " + pos, e);
            }
            if (map.put(key, val) != Long.MIN_VALUE) {
                throw new IllegalStateException("encountered repeated key at pos " + pos + ": " + key);
            }
            pos += Varint.computeSize(val);
        }
        if (chan.position() != chan.size()) {
            throw new IllegalStateException("scan incomplete at pos " + pos);
        }
        return map;
    }
}