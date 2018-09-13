package com.upserve.uppend;

public interface Trimmable {

    /**
     * Trim cached resources to reduce the heap usage
     */
    void trim();
}
