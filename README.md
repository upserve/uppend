Uppend: an append-only, key-multivalue store
============================================
[![Build Status](https://img.shields.io/travis/upserve/uppend/master.svg?style=flat-square)](https://travis-ci.org/upserve/uppend)
[![Release Artifact](https://img.shields.io/maven-central/v/com.upserve/uppend.svg?style=flat-square)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.upserve%22%20AND%20a%3Auppend)
[![Test Coverage](https://img.shields.io/codecov/c/github/upserve/uppend/master.svg?style=flat-square)](https://codecov.io/github/upserve/uppend?branch=master)
[![Release](https://jitpack.io/v/upserve/uppend.svg)](https://jitpack.io/#upserve/Uppend)

Uppend is an append-only, key-multivalue store which is suitable for streaming aggregation of immutable event data. 
It is designed to handle both wide (large number of unique keys) and deep (large number of elements in a particular 
key) data. Uppend is a multithreaded embedded java database. Uppend requires a single writer process but allows 
multiple reader processes sharing the same file system. Uppend guarantees you will read your writes from the writer 
process. New keys are not durably written immediately, they are added to a temporary cache. The write cache is 
periodically flushed to disk. Readonly processes are configured to re-read the key lookup data periodically. Uppend
heavily utilizes the mmap, and the linux page cache. On the i3.metal AWS instance class real Upserve applications
sustain millions of read and append operations per minute with median latency below 100 micro seconds on tables with
more than 1B keys.

Use
---

Maven:

```xml
<dependency>
    <groupId>com.upserve</groupId>
    <artifactId>uppend</artifactId>
    <version>0.3.0</version>
</dependency>
```

Gradle:
```gradle
compile 'com.upserve:uppend:0.3.0'
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

Diagrams
--------

AppendStore & CounterStore

![interface](images/uppend_interface.png)


Data Model
----------

Uppend stores four different kinds of data stored in four distinct files
* Arbitrary blobs of bytes are appended immediately in the order they are received. The starting position is all that is required 
  to read the blob again.
* Blocks of blob positions are allocated for each key and blob position are added to the block immediately. The starting position 
  of the first block for a particular key is all that is required to read the array of blob positions from a linked 
  list of blocks.
* The key and the starting position of the block appended to a blob store. These are held in a write cache until they are flushed.
* The starting position of each key is stored as an array of integers in lexical key sort order written during a flush.

![DataModel](https://docs.google.com/drawings/d/e/2PACX-1vTNVV_GxkaU_mvig5xLnIEtC2a1Aeheq5VokAp0aS_3OQYsNvsDMUiFPtlB2Vgyz3g9RE4SZWcYjOVF/pub?w=1182&h=374)

This is the contents of an AppendOnlyStore directory with 128 partitions

```
$ ls * partitions/0000
readLock	writeLock

partitions:
0000	0008	0016	0024	0032	0040	0048	0056	0064	0072	0080	0088	0096	0104	0112	0120
0001	0009	0017	0025	0033	0041	0049	0057	0065	0073	0081	0089	0097	0105	0113	0121
0002	0010	0018	0026	0034	0042	0050	0058	0066	0074	0082	0090	0098	0106	0114	0122
0003	0011	0019	0027	0035	0043	0051	0059	0067	0075	0083	0091	0099	0107	0115	0123
0004	0012	0020	0028	0036	0044	0052	0060	0068	0076	0084	0092	0100	0108	0116	0124
0005	0013	0021	0029	0037	0045	0053	0061	0069	0077	0085	0093	0101	0109	0117	0125
0006	0014	0022	0030	0038	0046	0054	0062	0070	0078	0086	0094	0102	0110	0118	0126
0007	0015	0023	0031	0039	0047	0055	0063	0071	0079	0087	0095	0103	0111	0119	0127

partitions/0000:
blobStore	blockedLongs	keyMetadata	keys
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