package com.upserve.uppend.lookup;

import java.nio.charset.StandardCharsets;

public class LookupKey implements Comparable<LookupKey> {
    private final byte[] bytes;
    // Lookup block index is immutable as the lookup blocks are append only
    private int insertAfterSortIndex;
    // Used to determine whether the sort order information is valid for this LookupKey during flush
    private int metaDataGeneration;

    public LookupKey(String stringValue) {
        if (stringValue == null) {
            throw new NullPointerException("null string given");
        }
        bytes = stringValue.getBytes(StandardCharsets.UTF_8);
        insertAfterSortIndex = -1;
    }

    public LookupKey(byte[] bytesValue){
        if (bytesValue == null) {
            throw new NullPointerException("null bytes given");
        }
        bytes = bytesValue;
        insertAfterSortIndex = -1;
    }

    public int getInsertAfterSortIndex(){
        return insertAfterSortIndex;
    }

    public void setInsertAfterSortIndex(int value){
        insertAfterSortIndex = value;
    }

    public int getMetaDataGeneration() {
        return metaDataGeneration;
    }

    public void setMetaDataGeneration(int metaDataGeneration) {
        this.metaDataGeneration = metaDataGeneration;
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

    // TODO optimize compareTo, equals and hashCode using the byte[]
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
