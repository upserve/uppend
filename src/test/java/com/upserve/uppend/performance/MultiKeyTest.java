package com.upserve.uppend.performance;

import com.upserve.uppend.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.*;

import static com.upserve.uppend.performance.StreamTimerMethods.*;

public class MultiKeyTest {

    private static final int numPartitions = 12;
    private static final int hashCount = 32;
    private static final int values = 100;
    private static final int blobsPerBlock = 64;
    private static final int keyCount = 1_000;

    private final Path path = Paths.get("build/test/tmp/performance/multi_key");
    private AppendOnlyStore appendOnlyStore;
    private final String[] keys = new String[keyCount]; // Just use a single key - it will be hashed appropriately for partition and hashsize
    private final int repeats = 5;

    private final Supplier<LongStream> parallelStream = () -> Arrays.stream(keys).flatMapToLong(key -> appendOnlyStore.read(key, key).mapToLong(bytes -> bytes.length).parallel()).parallel();
    private final Supplier<LongStream> sequentialStream = () -> Arrays.stream(keys).flatMapToLong(key -> appendOnlyStore.read(key, key).mapToLong(bytes -> bytes.length).sequential()).sequential();

    @Before
    public void loadStore() throws IOException {
        SafeDeleting.removeTempPath(path);

        appendOnlyStore = new AppendOnlyStoreBuilder()
                .withPartitionCount(numPartitions)
                .withLongLookupHashCount(hashCount)
                .withBlobsPerBlock(blobsPerBlock)
                .withDir(path).build();

        for (int value=0; value<keyCount; value++) {

            String key = String.format("%5d", value);
            keys[value] = key;
            new Random().ints(values, 0, 512)
                    .parallel()
                    .mapToObj(TestHelper::genBytes)
                    .forEach(bytes -> appendOnlyStore.append(key, key, bytes));
        }
    }

    @After
    public void cleanup() throws IOException {
        SafeDeleting.removeTempPath(path);
    }

    @Test
    public void sumTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(sum(parallelStream));
            sequentialTime(sum(sequentialStream));
        }
    }

    @Test
    public void forEachAdderTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(forEachAdder(parallelStream));
            sequentialTime(forEachAdder(sequentialStream));
        }
    }

    @Test
    public void groupByCountingTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(groupByCounting(parallelStream, false));
            sequentialTime(groupByCounting(sequentialStream, false));
        }
    }

    @Test
    public void concurrentGroupByCountingTest() {
        for (int i=0; i<repeats; i++) {
            parallelTime(groupByCounting(parallelStream, true));
            sequentialTime(groupByCounting(sequentialStream, true));
        }
    }
}
