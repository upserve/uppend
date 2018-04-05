package com.upserve.uppend.lookup;

import java.nio.charset.StandardCharsets;

public class LookupKey implements Comparable<LookupKey> {
    private final byte[] bytes;
    private boolean dirty;

    public LookupKey(byte[] bytesValue) {
        if (bytesValue == null) {
            throw new NullPointerException("null bytes given");
        }
        bytes = bytesValue;
        dirty = false;
    }

    public LookupKey(String stringValue) {
        if (stringValue == null) {
            throw new NullPointerException("null string given");
        }
        bytes = stringValue.getBytes(StandardCharsets.UTF_8);
    }

    public boolean isDirty() { return dirty; }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public String string() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public byte[] bytes() {
        return bytes;
    }

    public int byteLength() {
        return bytes.length;
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
