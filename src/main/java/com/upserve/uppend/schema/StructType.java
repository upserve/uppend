package com.upserve.uppend.schema;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class StructType implements Type {
    public String name;
    public SchemaField[] fields;

    public Map<String, Class> generateTypes() {
        String schemaDoc = colferSchema();
        // write schema doc to tmp file
        //   File.createTempFile()
        // write colfer binary to tmp file
        //   File.createTempFile()
        //   this.getClass().getClassLoader().getResourceAsStream("com/upserve/uppend/colfer/colf-<system>-1.5")
        // exec colfer binary like 'colf java <tmp schema file>'
        //   clean up schema doc and colfer binary
        // run javac on generated src
        //    (may require lrunning with -agentjar)
        //    clean up generated source
        // load classes into current classloader hierarchy
        //    clean up generated classfiles (if any)
        throw new RuntimeException("not yet implemented"); // TODO
    }

    private String colferSchema() {
        String generatedTimestamp = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss").format(new Date());
        StringBuilder schemaDoc = new StringBuilder();
        schemaDoc.append("// generated from com.upserve.uppend.schema.StructType: " + generatedTimestamp + "\npackage uppend\n");
        deepTypes().forEachOrdered(type -> {
            schemaDoc.append("\n");
            schemaDoc.append("type ").append(type.name).append(" struct {\n");
            for (SchemaField field : type.fields) {
                schemaDoc.append("    ").append(field.name).append(" ").append(field.type.fieldDeclaration()).append("\n");
            }
            schemaDoc.append("}\n");
        });
        return schemaDoc.toString();
    }

    private Stream<StructType> deepTypes() {
        return deepTypesInternal(null).values().stream();
    }

    private Map<String, StructType> deepTypesInternal(Map<String, StructType> typesByName) {
        if (typesByName == null) {
            typesByName = new TreeMap<>();
        }
        StructType existingType = typesByName.put(name, this);
        if (existingType != null) {
            if (existingType == this) {
                return typesByName;
            }
            throw new IllegalStateException("encountered two types with same name: " + this + "; " + existingType);
        }
        for (SchemaField field : fields) {
            Type fieldType = field.type;
            if (fieldType instanceof StructType) {
                StructType structFieldType = (StructType) fieldType;
                typesByName = structFieldType.deepTypesInternal(typesByName);
            }
        }
        return typesByName;
    }

    @Override
    public String fieldDeclaration() {
        return name;
    }
}
