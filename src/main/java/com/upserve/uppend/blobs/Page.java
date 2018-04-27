package com.upserve.uppend.blobs;

/**
 * Currently only MappedByteBuffer and FilePage are implemented - could implement on heap caching using
 * read once byte[]!
 */
public interface Page {
    int get(int pagePosition, byte[] dst, int bufferOffset);
    int put(int pagePosition, byte[] src, int bufferOffset);
}
