package com.upserve.uppend.cli;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.*;
import com.upserve.uppend.cli.benchmark.*;
import org.slf4j.Logger;
import picocli.CommandLine.*;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.concurrent.Callable;

@SuppressWarnings({"WeakerAccess", "unused"})
@Command(
        name = "benchmark",
        description = "Run store benchmark",
        showDefaultValues = true,
        synopsisHeading = "%nUsage: uppend ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n",
        commandListHeading = "%nCommands:%n",
        descriptionHeading = "%n",
        footerHeading = "%n"
)
public class CommandBenchmark implements Callable<Void> {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final String ROOT_NAME = "Root";
    public static final String STORE_NAME = "Benchmark";

    Benchmark benchmark;

    @Parameters(index = "0", description = "Store path")
    Path path;

    @Option(names = {"-m", "--mode"}, description = "Benchmark mode (read|write|readwrite|scan)")
    BenchmarkMode mode = BenchmarkMode.write;

    @Option(names = {"-s", "--size"}, description = "Benchmark size (nano|micro|small|medium|large|huge|gigantic)")
    BenchmarkSize size = BenchmarkSize.medium;

    @Option(names = {"-c", "--case"}, description = "Benchmark class (narrow|wide) key space")
    BenchmarkCase benchmarkCase = BenchmarkCase.narrow;

    @Option(names = {"-b", "--buffer-size"}, description = "Buffer Size (small|medium|large)")
    BufferSize bufferSize = BufferSize.medium;

    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Override
    public Void call() throws Exception {

        if (Files.exists(path)) {
            log.warn("Location already exists: appending to {}", path);
        }

        benchmark = createBenchmark();
        benchmark.run();
        return null;
    }

    private Benchmark createBenchmark() {
        long keys;
        long count;

        final int blockSize;
        int partitionCount;
        int hashCount;

        int blobCacheSize;
        int blobPageSize;

        int keyPageCacheSize;
        int keyPageSize;

        int metadataCacheSize;
        int metadataPageSize;
        long metadataCacheWeight;

        int flushThreshold;
        int flushDelay;

        switch (benchmarkCase) {
            case narrow:
                count = size.getSize();
                keys = (long) Math.pow(Math.log10(count), 2.0) * 100;

                blockSize = 16_384;
                partitionCount = 64;
                hashCount = 64;

                blobPageSize = 16 * 1024 * 1024;
                keyPageSize = 1024 * 1024;
                metadataPageSize = 1024 * 1024;

                flushDelay = 60;
                flushThreshold = -1;

                break;

            case wide:
                keys = size.getSize();
                count = keys * 2;

                blockSize = 4;
                hashCount = 256;
                partitionCount = 128;

                blobPageSize = 64 * 1024; // Pages will roll over at 135M keys
                keyPageSize = 4 * 1024; // Key pages will roll over at about 2.9B keys
                metadataPageSize = 4 * 1024;

                flushDelay = -1;
                flushThreshold = 256;

                break;

            default:
                throw new IllegalStateException("Ensure variables are initialized");
        }

        MetricRegistry metrics = new MetricRegistry();

        AppendOnlyStoreBuilder builder = Uppend.store(path)
                .withStoreName(STORE_NAME)
                .withMetricsRootName(ROOT_NAME)
                .withBlobsPerBlock(blockSize)
                .withLongLookupHashCount(hashCount)
                .withPartitionCount(partitionCount) // Use direct partition
                .withTargetBufferSize(bufferSize.getSize())
                .withBlobPageSize(blobPageSize)
                .withLookupPageSize(keyPageSize)
                .withMetadataPageSize(metadataPageSize)
                .withFlushThreshold(flushThreshold)
                .withFlushDelaySeconds(flushDelay)
                .withStoreMetrics(metrics);

        return new Benchmark(mode, builder, keys, count);
    }
}
