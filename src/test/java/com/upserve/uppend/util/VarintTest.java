package com.upserve.uppend.util;

import org.junit.*;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class VarintTest {
    @Test
    public void encodedBytesSizes() {
        assertEquals(1, varintBytesSizeof(0));
        assertEquals(1, varintBytesSizeof(1));
        assertEquals(1, varintBytesSizeof(2));
        assertEquals(1, varintBytesSizeof(3));
        assertEquals(1, varintBytesSizeof(4));
        assertEquals(1, varintBytesSizeof(7));
        assertEquals(1, varintBytesSizeof(8));
        assertEquals(1, varintBytesSizeof(15));
        assertEquals(1, varintBytesSizeof(16));
        assertEquals(1, varintBytesSizeof(31));
        assertEquals(1, varintBytesSizeof(32));
        assertEquals(1, varintBytesSizeof(63));
        assertEquals(1, varintBytesSizeof(64));
        assertEquals(1, varintBytesSizeof(127));

        assertEquals(2, varintBytesSizeof(128));
        assertEquals(2, varintBytesSizeof(255));
        assertEquals(2, varintBytesSizeof(256));
        assertEquals(2, varintBytesSizeof(511));
        assertEquals(2, varintBytesSizeof(512));
        assertEquals(2, varintBytesSizeof(1023));
        assertEquals(2, varintBytesSizeof(1024));
        assertEquals(2, varintBytesSizeof(2047));
        assertEquals(2, varintBytesSizeof(2048));
        assertEquals(2, varintBytesSizeof(4095));
        assertEquals(2, varintBytesSizeof(4096));
        assertEquals(2, varintBytesSizeof(8191));
        assertEquals(2, varintBytesSizeof(8192));
        assertEquals(2, varintBytesSizeof(16383));

        assertEquals(3, varintBytesSizeof(16384));
        assertEquals(3, varintBytesSizeof(2097151));

        assertEquals(4, varintBytesSizeof(2097152));
        assertEquals(4, varintBytesSizeof(268435455));

        assertEquals(5, varintBytesSizeof(268435456));
        assertEquals(5, varintBytesSizeof(34359738367L));

        assertEquals(6, varintBytesSizeof(34359738368L));
        assertEquals(6, varintBytesSizeof(4398046511103L));

        assertEquals(7, varintBytesSizeof(4398046511104L));
        assertEquals(7, varintBytesSizeof(562949953421311L));

        assertEquals(8, varintBytesSizeof(562949953421312L));
        assertEquals(8, varintBytesSizeof(72057594037927935L));

        assertEquals(9, varintBytesSizeof(72057594037927936L));
        assertEquals(9, varintBytesSizeof(9223372036854775807L));

        assertEquals(10, varintBytesSizeof(-1));
        assertEquals(10, varintBytesSizeof(Long.MIN_VALUE));
    }

    @Test
    public void computedBytesSizes() {
        assertEquals(1, Varint.computeSize(0));
        assertEquals(1, Varint.computeSize(1));
        assertEquals(1, Varint.computeSize(2));
        assertEquals(1, Varint.computeSize(3));
        assertEquals(1, Varint.computeSize(4));
        assertEquals(1, Varint.computeSize(7));
        assertEquals(1, Varint.computeSize(8));
        assertEquals(1, Varint.computeSize(15));
        assertEquals(1, Varint.computeSize(16));
        assertEquals(1, Varint.computeSize(31));
        assertEquals(1, Varint.computeSize(32));
        assertEquals(1, Varint.computeSize(63));
        assertEquals(1, Varint.computeSize(64));
        assertEquals(1, Varint.computeSize(127));

        assertEquals(2, Varint.computeSize(128));
        assertEquals(2, Varint.computeSize(255));
        assertEquals(2, Varint.computeSize(256));
        assertEquals(2, Varint.computeSize(511));
        assertEquals(2, Varint.computeSize(512));
        assertEquals(2, Varint.computeSize(1023));
        assertEquals(2, Varint.computeSize(1024));
        assertEquals(2, Varint.computeSize(2047));
        assertEquals(2, Varint.computeSize(2048));
        assertEquals(2, Varint.computeSize(4095));
        assertEquals(2, Varint.computeSize(4096));
        assertEquals(2, Varint.computeSize(8191));
        assertEquals(2, Varint.computeSize(8192));
        assertEquals(2, Varint.computeSize(16383));

        assertEquals(3, Varint.computeSize(16384));
        assertEquals(3, Varint.computeSize(2097151));

        assertEquals(4, Varint.computeSize(2097152));
        assertEquals(4, Varint.computeSize(268435455));

        assertEquals(5, Varint.computeSize(268435456));
        assertEquals(5, Varint.computeSize(34359738367L));

        assertEquals(6, Varint.computeSize(34359738368L));
        assertEquals(6, Varint.computeSize(4398046511103L));

        assertEquals(7, Varint.computeSize(4398046511104L));
        assertEquals(7, Varint.computeSize(562949953421311L));

        assertEquals(8, Varint.computeSize(562949953421312L));
        assertEquals(8, Varint.computeSize(72057594037927935L));

        assertEquals(9, Varint.computeSize(72057594037927936L));
        assertEquals(9, Varint.computeSize(9223372036854775807L));

        assertEquals(10, Varint.computeSize(-1));
        assertEquals(10, Varint.computeSize(Long.MIN_VALUE));
    }

    @Test
    public void rountrip() {
        long[] vals = {
                0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127,
                128, 255, 256, 511, 512, 1023, 1024, 2047, 2048, 4095, 4096, 8191, 8192, 16383,
                16384, 2097151,
                2097152, 268435455,
                268435456, 34359738367L,
                34359738368L, 4398046511103L,
                4398046511104L, 562949953421311L,
                562949953421312L, 72057594037927935L,
                72057594037927936L, 9223372036854775807L,
                -1, Long.MIN_VALUE
        };

        for (long val : vals) {
            assertEquals(val, varintRoundtrip(val));
        }
    }

    private static int varintBytesSizeof(long value) {
        return varintBytes(value).length;
    }

    private static byte[] varintBytes(long value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Varint.write(baos, value);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to write to byte array output stream: " + value, e);
        }
        return baos.toByteArray();
    }

    private static long varintRoundtrip(long value) {
        byte[] bytes = varintBytes(value);
        try {
            return Varint.readLong(new ByteArrayInputStream(bytes));
        } catch (IOException e) {
            throw new UncheckedIOException("unable to read from byte array: " + value, e);
        }
    }
}
