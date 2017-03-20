package com.upserve.uppend.schema;

public enum PrimitiveType implements Type {
    bool,
    uint8,
    uint16,
    uint32,
    uint64,
    int32,
    int64,
    float32,
    float64,
    timestamp,
    text,
    binary;


    @Override
    public String fieldDeclaration() {
        return name();
    }
}
