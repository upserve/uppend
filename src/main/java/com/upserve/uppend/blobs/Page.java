package com.upserve.uppend.blobs;

import static java.lang.Integer.min;

/**
 * Currently only MappedByteBuffer and FilePage are implemented - could implement on heap caching using
 * read once byte[]!
 */
public interface Page {
    int get(int pagePosition, byte[] dst, int bufferOffset);

    int put(int pagePosition, byte[] src, int bufferOffset);

    default int actualOperationSize(int pagePosition, int pagesize, int bufferOffset, int bufferLength) {
        final int desiredRead = bufferLength - bufferOffset;
        final int availableToRead = pagesize - pagePosition;
        return min(desiredRead, availableToRead);
    }
}
