package com.upserve.uppend.lookup;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LookupKey implements Comparable<LookupKey> {
    private final byte[] bytes;
    // Lookup block index is immutable as the lookup blocks are append only
    private int insertAfterSortIndex;
    // Used to determine whether the sort order information is valid for this LookupKey during flush
    private int metaDataGeneration;
    // the position of this key in the longBlob file
    private int position;

    public LookupKey(String stringValue) {
        if (stringValue == null) {
            throw new NullPointerException("null string given");
        }

        for (char c : stringValue.toCharArray()) {
            if (c > 128)
                throw new IllegalArgumentException("The key '" + stringValue + "' contains a non ascii character: " + c);
        }

        bytes = stringValue.getBytes(StandardCharsets.US_ASCII);
        insertAfterSortIndex = -1;
        position = -1;
    }

    public LookupKey(byte[] bytesValue) {
        if (bytesValue == null) {
            throw new NullPointerException("null bytes given");
        }
        bytes = bytesValue;
        insertAfterSortIndex = -1;
        position = -1;
    }

    int getInsertAfterSortIndex() {
        return insertAfterSortIndex;
    }

    void setInsertAfterSortIndex(int value) {
        insertAfterSortIndex = value;
    }

    int getMetaDataGeneration() {
        return metaDataGeneration;
    }

    void setMetaDataGeneration(int metaDataGeneration) {
        this.metaDataGeneration = metaDataGeneration;
    }

    public String string() {
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    int getPosition() {
        return position;
    }

    void setPosition(int position) {
        this.position = position;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int byteLength() {
        return bytes.length;
    }

    @Override
    public int compareTo(LookupKey o) {
        return Arrays.compare(bytes, o.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LookupKey lookupKey = (LookupKey) o;

        return Arrays.equals(bytes, lookupKey.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return string();
    }
}
