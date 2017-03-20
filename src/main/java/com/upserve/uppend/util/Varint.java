package com.upserve.uppend.util;

import java.io.*;

/**
 * Support for varint encoding, as defined by Protocol Buffers. Note: currently
 * supports only long (int64), and does not use ZigZag encoding, so negative
 * numbers are always encoded as 10 bytes.
 *
 * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">https://developers.google.com/protocol-buffers/docs/encoding#varints</a>
 */
public class Varint {
    private static final long[] MAX_VARINT_AT_SIZE = new long[9];
    static {
        for (int size = 1; size < 9; size++) {
            // every byte contains 7 bits of value plus one bit for continuation
            MAX_VARINT_AT_SIZE[size] = (1L << (7 * size)) - 1;
        }
    }

    /**
     * Encode a long value as a varint and write it to an {@code OutputStream}.
     *
     * @param out the output stream to write to
     * @param value the value to encode as a varint
     * @throws IOException when there is trouble writing to the stream
     */
    public static void write(OutputStream out, long value) throws IOException {
        while (value < 0 || value > 127) {
            out.write((int) (128 | value & 127));
            value >>>= 7;
        }
        out.write((int) value);
    }

    /**
     * Consume an encoded varint from the given {@code InputStream} and return
     * its value as a 64 bit long.
     *
     * @param in the input stream to read from
     * @return the value of varint
     * @throws IOException when there is trouble reading a varint from the
     * stream
     */
    public static long readLong(InputStream in) throws IOException {
        long value = 0;
        for(int shift = 0; shift < 64; shift += 7) {
            int b = in.read();
            value |= (long) (b & 127) << shift;
            if (b < 128) {
                return value;
            }
        }
        throw new IOException("malformed varint in stream");
    }

    /**
     * Computes the size of varint encoding for a given value.
     *
     * @param value value that would be encoded
     * @return number of bytes used when encoded as a varint
     */
    public static int computeSize(final long value) {
        if (value < 0) {
            return 10;
        }
        for (int size = 1; size < 9; size++) {
            if (MAX_VARINT_AT_SIZE[size] >= value) {
                return size;
            }
        }
        return 9;
    }
}
