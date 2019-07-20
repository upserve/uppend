package com.upserve.uppend.cli;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.cli.benchmark.*;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

@SuppressWarnings({"WeakerAccess", "unused"})
@CommandLine.Command(
        name = "filestore",
        description = "Run file store benchmark",
        showDefaultValues = true,
        synopsisHeading = "%nUsage: uppend ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n",
        commandListHeading = "%nCommands:%n",
        descriptionHeading = "%n",
        footerHeading = "%n"
)
public class CommandFileStoreBenchmark implements Callable<Void> {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private IntSummaryStatistics result;

    @CommandLine.Parameters(index = "0", description = "Store path")
    Path path;

    @CommandLine.Option(names= {"-n", "--virtual-files"}, description = "the number of virtual files to use")
    int nfiles = 32;

    @CommandLine.Option(names = {"-b", "--buffer-size"}, description = "Buffer Size (small|medium|large)")
    BufferSize bufferSize = BufferSize.medium;

    @CommandLine.Option(names = {"-s", "--size"}, description = "Benchmark size (nano|micro|small|medium|large|huge|gigantic)")
    BenchmarkSize size = BenchmarkSize.medium;

    @CommandLine.Option(names = {"-p", "--page-size"}, description = "Page Size (small|medium|large)")
    PageSize pageSize = PageSize.medium.medium;

    @SuppressWarnings("unused")
    @CommandLine.Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    public IntSummaryStatistics getStats() {
        return result;
    }

    @Override
    public Void call() throws Exception {

        log.info("FileStore Benchmark");

        Random random = new Random();

        Files.createDirectories(path.getParent());

        VirtualPageFile file = new VirtualPageFile(path, nfiles, pageSize.getSize(), bufferSize.getSize(), false);
        VirtualAppendOnlyBlobStore[] stores = IntStream.range(0, nfiles)
                .mapToObj(val -> new VirtualAppendOnlyBlobStore(val, file))
                .toArray(VirtualAppendOnlyBlobStore[]::new);


        long tic = System.currentTimeMillis();

        result = ThreadLocalRandom.current()
                .ints(size.getSize(), 0, nfiles)
                .parallel()
                .map(val -> {

                    byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(1024)];
                    random.nextBytes(bytes);

                    stores[val].append(bytes);
                    return bytes.length;
                }).summaryStatistics();

        long toc = System.currentTimeMillis();

        log.info("[All Done!] {} ms", toc - tic);
        return null;
    }
}
