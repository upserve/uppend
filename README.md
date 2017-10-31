Uppend: an append-only, key-multivalue store 
============================================
[![Build Status](https://travis-ci.com/upserve/uppend.svg?token=dpSeApDGQn1qo5LYyLJx&branch=add_travis_yml)](https://travis-ci.com/upserve/uppend)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.upserve/uppend/badge.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.upserve%22%20AND%20a%3Auppend)

Uppend is an append-only, key-multivalue store which is suitable for streaming
event aggregation. It assumes a single writer process, and appended values are
immutable.

Use
---

Maven:

```xml
<dependency>
    <groupId>com.upserve</groupId>
    <artifactId>uppend</artifactId>
    <version>1.0.0</version>
</dependency>
```

Gradle:
```gradle
compile 'com.upserve:uppend:1.0.0'
```

Hello world:

```java
AppendOnlyStore db = Uppend.fileStore("tmp/uppend").build();

db.append("my-partition", "my-key", "value-1".getBytes());
db.append("my-partition", "my-key", "value-2".getBytes());

db.flush();

String values = db.readSequential("my-partition", "my-key")
        .map(String::new)
        .collect(Collectors.joining(", "));
// value-1, value-2
```

Development
-----------

To build Uppend, run:
 
```sh
./gradlew build
```
