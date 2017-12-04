package com.upserve.uppend;

import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class FileStore implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * DEFAULT_FLUSH_DELAY_SECONDS is the number of seconds to wait between
     * automatically flushing writes.
     */
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;

    protected final Path dir;

    private final int flushDelaySeconds;

    private final boolean doLock;
    private final Path lockPath;
    private final FileChannel lockChan;
    private final FileLock lock;

    private final AtomicBoolean isClosed;

    FileStore(Path dir, int flushDelaySeconds, boolean doLock) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        this.flushDelaySeconds = flushDelaySeconds;
        if (flushDelaySeconds > 0) {
            AutoFlusher.register(flushDelaySeconds, this);
        }

        this.doLock = doLock;
        if (doLock) {
            lockPath = dir.resolve("lock");
            try {
                lockChan = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                lock = lockChan.lock();
            } catch (IOException e) {
                throw new UncheckedIOException("unable to open lock: " + lockPath, e);
            } catch (OverlappingFileLockException e) {
                throw new IllegalStateException("lock busy: " + lockPath, e);
            }
        } else {
            lockPath = null;
            lockChan = null;
            lock = null;
        }

        isClosed = new AtomicBoolean(false);
    }

    protected abstract void flushInternal();

    protected abstract void closeInternal();

    @Override
    public void flush() {
        log.info("flushing {}", dir);
        try {
            flushInternal();
        } catch (Exception e) {
            log.error("unable to flush", e);
        }
        log.info("flushed {}", dir);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            log.warn("close called twice on file store: " + dir);
            return;
        }

        if (flushDelaySeconds > 0) {
            AutoFlusher.deregister(this);
        }

        try {
            closeInternal();
        } catch (Exception e) {
            log.error("unable to close", e);
        }

        if (doLock) {
            try {
                lock.release();
            } catch (IOException e) {
                log.error("unable to release lock file: " + lockPath, e);
            }
            try {
                lockChan.close();
            } catch (IOException e) {
                log.error("unable to close lock file: " + lockPath, e);
            }
            try {
                Files.deleteIfExists(lockPath);
            } catch (IOException e) {
                log.error("unable to delete lock file: " + lockPath, e);
            }
        }
    }
}
