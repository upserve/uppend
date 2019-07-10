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
import static com.upserve.uppend.performance.StreamTimerMethods.flatMapSum;

public class SingleKeyTest {

    private static final int partitionCount = 4;
    private static final int hashCount = 4;
    private static final int values = 1_000_000;
    private static final int blobsPerBlock = 4096;
    private final Path path = Paths.get("build/test/tmp/performance/single_key");
    private AppendOnlyStore appendOnlyStore;
    private String partition = "foo";
    private String key = "bar";
    private final int repeats = 5;

    private final Supplier<LongStream> parallelStream = () -> appendOnlyStore.read(partition, key).mapToLong(bytes -> bytes.length).parallel();
    private final Supplier<LongStream> sequentialStream = () -> appendOnlyStore.read(partition, key).mapToLong(bytes -> bytes.length).sequential();

    @Before
    public void loadStore() throws IOException {
        SafeDeleting.removeTempPath(path);

        appendOnlyStore = new AppendOnlyStoreBuilder()
                .withPartitionCount(partitionCount)
                .withLongLookupHashCount(hashCount)
                .withBlobsPerBlock(blobsPerBlock)
                .withDir(path).build();

        new Random()
                .ints(values, 0, 512)
                .parallel()
                .mapToObj(TestHelper::genBytes)
                .forEach(bytes -> appendOnlyStore.append(partition, key, bytes));
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

    @Test
    public void flatMapTest(){
        for (int i=0; i<repeats; i++) {
            parallelTime(flatMapSum(
                    Stream.of(1,2,3,4,5).flatMap(val -> appendOnlyStore.read(partition, key)).mapToLong(bytes -> bytes.length).parallel()
            ));
            sequentialTime(flatMapSum(
                    Stream.of(1,2,3,4,5).flatMap(val -> appendOnlyStore.read(partition, key)).mapToLong(bytes -> bytes.length).sequential()
            ));
        }
    }
}
