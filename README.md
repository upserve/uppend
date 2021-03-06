Uppend: an append-only, key-multivalue store
============================================
[![Build Status](https://img.shields.io/travis/upserve/uppend/master.svg?style=flat-square)](https://travis-ci.org/upserve/uppend)
[![Release Artifact](https://img.shields.io/maven-central/v/com.upserve/uppend.svg?style=flat-square)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.upserve%22%20AND%20a%3Auppend)
[![Test Coverage](https://img.shields.io/codecov/c/github/upserve/uppend/master.svg?style=flat-square)](https://codecov.io/github/upserve/uppend?branch=master)
[![Release](https://jitpack.io/v/upserve/uppend.svg)](https://jitpack.io/#upserve/Uppend)

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
    <version>0.2.1</version>
</dependency>
```

Gradle:
```gradle
compile 'com.upserve:uppend:0.2.1'
```

Hello world:

```java
AppendOnlyStore db = Uppend.store("build/tmp-db").build();

db.append("my-appendStorePartition", "my-key", "value-1".getBytes());
db.append("my-appendStorePartition", "my-key", "value-2".getBytes());

String values = db.readSequential("my-appendStorePartition", "my-key")
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

To benchmark Uppend:

```sh
./gradlew clean fatJar
java -jar build/libs/uppend-all-*.jar --help
```

To run tests without verbose output
```sh
 ./gradlew test -i
```

To run tests in a specific path
```sh
 ./gradlew test --tests com.upserve.uppend.blobs*
```

Example script to fork the benchmark with a system resource monitor like IOSTAT

_runtest.sh_
```sh
trap "kill 0" EXIT

java -Xmx32g -jar ./uppend-all-0.2.1.jar benchmark -c $C -m $M -s $S -b $B ./data1.output & BENCHMARK_PID=$!

iostat -c -d 5 -x & IOSTAT_PID=$!

wait $BENCHMARK_PID
kill $IOSTAT_PID
```

Call _runtest.sh_ with:
```sh
export C=wide
export M=read
export S=large
export B=medium
./runtest.sh 2>&1 | tee /mnt/log/${M}_${C}_${S}_${B}.log
```