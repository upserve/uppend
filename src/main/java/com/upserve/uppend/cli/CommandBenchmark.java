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


    @Parameters(index = "0", description = "Store path")
    Path path;

    @Option(names = {"-m", "--mode"}, description = "Benchmark mode (read|write|readwrite|scan)")
    BenchmarkMode mode = BenchmarkMode.write;

    @Option(names = {"-s", "--size"}, description = "Benchmark size (small|medium|large|huge)")
    BenchmarkSize size = BenchmarkSize.medium;

    @Option(names = {"-c", "--case"}, description = "Benchmark class (narrow|wide) key space")
    BenchmarkCase benchmarkCase = BenchmarkCase.narrow;

    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Override
    public Void call() throws Exception {

        if (Files.exists(path)) {
            log.warn("Location already exists: appending to {}", path);
        }

        Benchmark benchmark = createBenchmark();
        benchmark.run();
        return null;
    }


    private Benchmark createBenchmark() {
        long keys;
        long count;

        final int blockSize;
        int partitions;
        int hashSize;

        int keyCacheSize;
        long keyCacheWeight;

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
                partitions = 64;
                hashSize = 64;

                // Cache all the keys
                keyCacheSize = (int) keys;
                keyCacheWeight = keys * 9 + 1000; // 9 bytes per key plus some room

                blobCacheSize = partitions * hashSize;
                blobPageSize = 16 * 1024 * 1024;

                keyPageCacheSize = partitions * hashSize;
                keyPageSize = 1024 * 1024;

                metadataCacheSize = partitions * hashSize;
                metadataCacheWeight = keys * 4 + 1000; // one int per key plus some room
                metadataPageSize = 1024 * 1024;

                flushDelay = 60;
                flushThreshold = -1;

                break;

            case wide:
                keys = size.getSize();
                count = keys * 2;

                blockSize = 4;
                hashSize = 1024;
                partitions = (int) Math.min(1024, keys / (64 * hashSize));

                keyCacheSize = 0;
                keyCacheWeight = 0;

                blobCacheSize = 16 * hashSize;
                blobPageSize = 1024 * 1024;

                keyPageCacheSize = partitions * hashSize;
                keyPageSize = 1024 * 1024;

                metadataCacheSize = partitions * hashSize;
                metadataCacheWeight = 4 * keys + 10_000; // one int per key plus some room
                metadataPageSize = 1024 * 1024;

                flushDelay = 0;
                flushThreshold = 32;

                break;

            default:
                throw new IllegalStateException("Ensure variables are initialized");
        }

        MetricRegistry metrics = new MetricRegistry();


        AppendOnlyStoreBuilder builder = Uppend.store(path)
                .withStoreName(STORE_NAME)
                .withMetricsRootName(ROOT_NAME)

                .withBlobsPerBlock(blockSize)
                .withLongLookupHashSize(hashSize)
                .withPartitionSize(partitions) // Use direct partition

                .withInitialLookupKeyCacheSize(keyCacheSize)
                .withMaximumLookupKeyCacheWeight(keyCacheWeight)

                .withInitialBlobCacheSize(blobCacheSize)
                .withMaximumBlobCacheSize(blobCacheSize)
                .withBlobPageSize(blobPageSize)

                .withInitialLookupPageCacheSize(keyPageCacheSize)
                .withMaximumLookupPageCacheSize(keyPageCacheSize)
                .withLookupPageSize(keyPageSize)

                .withInitialMetaDataCacheSize(metadataCacheSize)
                .withMetaDataPageSize(metadataPageSize)
                .withMaximumMetaDataCacheWeight(metadataCacheWeight)

                .withFlushThreshold(flushThreshold)
                .withFlushDelaySeconds(flushDelay)

                .withStoreMetrics(metrics)
                .withCacheMetrics();

        return new Benchmark(mode, builder, partitions, keys, count);
    }
}
