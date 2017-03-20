package com.upserve.uppend.schema;

public class ListType implements Type {
    public Type elementType;

    @Override
    public String fieldDeclaration() {
        return "[]" + elementType.fieldDeclaration();
    }
}
