package com.upserve.uppend;

import java.io.Flushable;

public interface RegisteredFlushable extends Flushable {

    /**
     * Register this flushable with scheduled flusher
     * @param seconds seconds between flushes
     */
    void register(int seconds);

    /**
     * Deregister this flushable with scheduled flusher
     */
    void deregister();
}
