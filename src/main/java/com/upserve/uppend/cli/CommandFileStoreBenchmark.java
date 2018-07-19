package com.upserve.uppend.cli;

import com.upserve.uppend.blobs.*;
import com.upserve.uppend.cli.benchmark.*;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.*;

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

    @CommandLine.Parameters(index = "0", description = "Store path")
    Path path;

    @CommandLine.Option(names= {"-n", "--virtual-files"}, description = "the number of virtual files to use")
    int nfiles = 32;

    @CommandLine.Option(names = {"-b", "--buffer-size"}, description = "Buffer Size (small|medium|large)")
    BufferSize bufferSize = BufferSize.medium;

    @CommandLine.Option(names = {"-s", "--size"}, description = "Benchmark size (nano|micro|small|medium|large|huge|gigantic)")
    BenchmarkSize size = BenchmarkSize.medium;

    @Override
    public Void call() throws Exception {

        log.info("FileStore Benchmark");

        Random random = new Random();

        VirtualPageFile file = new VirtualPageFile(path, nfiles, 2097152, bufferSize.getSize(), false);
        VirtualAppendOnlyBlobStore[] stores = IntStream.range(0, nfiles)
                .mapToObj(val -> new VirtualAppendOnlyBlobStore(val, file))
                .toArray(VirtualAppendOnlyBlobStore[]::new);

        Random randomSize = new Random();

        long tic = System.currentTimeMillis();

        random
                .ints(size.getSize(), 0, nfiles)
                .parallel()
                .forEach(val -> {

                    byte[] bytes = new byte[randomSize.nextInt(64)];
                    random.nextBytes(bytes);

                    stores[val].append(bytes);
                });

        long toc = System.currentTimeMillis();

        log.info("[done!] {} ms", toc - tic);

        return null;
    }
}
