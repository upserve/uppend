package com.upserve.uppend.cli;

import com.upserve.uppend.AppendOnlyStoreBuilder;
import com.upserve.uppend.cli.benchmark.*;
import picocli.CommandLine.*;

import java.nio.file.Path;
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
    @Parameters(index = "0", description = "Benchmark mode (read|write|readwrite|scan)")
    BenchmarkMode mode;
    @Parameters(index = "1", description = "Store path")
    Path path;

    @Option(names = {"-p", "--max-partitions"}, description = "Max partitions")
    int maxPartitions = 1;

    @Option(names = {"-k", "--max-keys"}, description = "Max keys")
    int maxKeys = 100_000;

    @Option(names = {"-n", "--count"}, description = "Count")
    int count = 1_000_000;

    @Option(names = {"-h", "--hash-size"}, description = "Hash size")
    int hashSize = AppendOnlyStoreBuilder.DEFAULT_LOOKUP_HASH_SIZE;

    @Option(names = {"-c", "--key-cache-size"}, description = "Key Cache size")
    int keyCacheSize = AppendOnlyStoreBuilder.DEFAULT_INITIAL_LOOKUP_KEY_CACHE_SIZE * 100;

    @Option(names = {"-m", "--key-metadata-cache-size"}, description = "Metadata Cache size")
    int metadataCacheSize = AppendOnlyStoreBuilder.DEFAULT_INITIAL_METADATA_CACHE_SIZE;

    @Option(names = {"-b", "--blob-page-cache-size"}, description = "Blob Page Cache size")
    int blobPageCacheSize = AppendOnlyStoreBuilder.DEFAULT_MAXIMUM_CACHED_BLOB_PAGES;

    @Option(names = {"-s", "--key-page-cache-size"}, description = "Key Page Cache size")
    int keyPageCacheSize = AppendOnlyStoreBuilder.DEFAULT_INITIAL_LOOKUP_PAGE_CACHE_SIZE;

    @Option(names = {"-o", "--metadata-page-size"}, description = "Metadata page size")
    int metadataPageSize = AppendOnlyStoreBuilder.DEFAULT_METADATA_PAGE_SIZE;

    @Option(names = {"-f", "--flush-delay"}, description = "Flush delay (sec)")
    int flushDelay = AppendOnlyStoreBuilder.DEFAULT_FLUSH_DELAY_SECONDS;

    @Option(names = {"-d", "--flush-threshold"}, description = "Flush Threshold Size")
    int flushThreshold = AppendOnlyStoreBuilder.DEFAULT_FLUSH_THRESHOLD;

    @Option(names = {"-l", "--block-size"}, description = "Block Size")
    int blockSize = AppendOnlyStoreBuilder.DEFAULT_BLOBS_PER_BLOCK;

    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Override
    public Void call() throws Exception {
        Benchmark benchmark = new Benchmark(
                mode, path, maxPartitions, maxKeys, count, hashSize, keyCacheSize, metadataCacheSize, metadataPageSize, blobPageCacheSize, keyPageCacheSize, flushDelay, blockSize, flushThreshold
        );
        benchmark.run();
        return null;
    }
}
