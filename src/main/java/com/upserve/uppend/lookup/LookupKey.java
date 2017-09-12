package com.upserve.uppend.lookup;

import java.nio.charset.StandardCharsets;

public class LookupKey implements Comparable<LookupKey> {
    private byte[] bytes;
    private int byteLength = -1;
    private String string;
    private int stringLength = -1;

    public LookupKey(byte[] bytesValue) {
        if (bytesValue == null) {
            throw new NullPointerException("null bytes given");
        }
        bytes = bytesValue;
    }

    public LookupKey(String stringValue) {
        if (stringValue == null) {
            throw new NullPointerException("null string given");
        }
        string = stringValue;
    }

    public String string() {
        if (string == null) {
            string = new String(bytes, StandardCharsets.UTF_8);
        }
        return string;
    }

    public int stringLength() {
        if (stringLength == -1) {
            stringLength = string().length();
        }
        return stringLength;
    }

    public byte[] bytes() {
        if (bytes == null) {
            bytes = string.getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    public int byteLength() {
        if (byteLength == -1) {
            byteLength = bytes().length;
        }
        return byteLength;
    }

    @Override
    public int compareTo(LookupKey o) {
        return string().compareTo(o.string());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LookupKey lookupKey = (LookupKey) o;

        return string().equals(lookupKey.string());
    }

    @Override
    public int hashCode() {
        return string().hashCode();
    }

    @Override
    public String toString() {
        return string();
    }
}
