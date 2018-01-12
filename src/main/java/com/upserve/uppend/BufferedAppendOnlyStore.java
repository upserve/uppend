package com.upserve.uppend;

import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * The Buffered Append Only Store is optimized for random writes to the partition and key with fixed memory use at the
 * expense of durability and latency. No guarantee is made about when an append operation will be durably persisted or available
 * to read. The application must successfully call flush or close to persist the appended values.
 *
 * By default the provided buffer size is a suggestion. The buffer is actually flushed based on the number of entries
 * with entropy in a range equal the the suggested size divided by 5.
 */
public class BufferedAppendOnlyStore extends FileAppendOnlyStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LookupAppendBuffer lookupAppendBuffer;

    BufferedAppendOnlyStore(Path dir, boolean doLock, int longLookupHashSize, int bufferSize, Optional<ExecutorService> executorService) {
        super(dir, 0, doLock, longLookupHashSize, 0);
        lookupAppendBuffer = new LookupAppendBuffer(lookups, blocks, bufferSize, bufferSize/5, executorService);
    }

    @Override
    public void append(String partition, String key, byte[] value) {
        log.trace("buffered append for key '{}'", key);
        long blobPos = blobs.append(value);
        lookupAppendBuffer.bufferedAppend(partition, key, blobPos);
    }

    @Override
    protected void flushInternal() {
        lookupAppendBuffer.flush();
        super.flushInternal();
    }

    @Override
    protected void closeInternal() {
        lookupAppendBuffer.close();
        super.closeInternal();
    }

    @Override
    public void clear(){
        lookupAppendBuffer.clearLock();
        super.clear();
        lookupAppendBuffer.unlock();
    }
}
