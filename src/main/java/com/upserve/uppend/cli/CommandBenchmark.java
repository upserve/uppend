package com.upserve.uppend.cli;

import com.upserve.uppend.cli.benchmark.*;
import com.upserve.uppend.lookup.LongLookup;
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
    @Parameters(index = "0", description = "Benchmark mode (read|write|readwrite)") BenchmarkMode mode;
    @Parameters(index = "1", description = "Store path") Path path;

    @Option(names = {"-p", "--max-partitions"}, description = "Max partitions")
    int maxPartitions = 64;

    @Option(names = {"-k", "--max-keys"}, description = "Max keys")
    int maxKeys = 1000;

    @Option(names = {"-n", "--count"}, description = "Count")
    int count = 100_000;

    @Option(names = {"-h", "--hash-size"}, description = "Hash size")
    int hashSize = LongLookup.DEFAULT_HASH_SIZE;

    @Option(names = {"-c", "--cache-size"}, description = "Cache size")
    int cacheSize = LongLookup.DEFAULT_WRITE_CACHE_SIZE;

    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Override
    public Void call() throws Exception {
        Benchmark benchmark = new Benchmark(mode, path, maxPartitions, maxKeys, count, hashSize, cacheSize);
        benchmark.run();
        return null;
    }
}
