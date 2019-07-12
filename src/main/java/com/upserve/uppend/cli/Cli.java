package com.upserve.uppend.cli;

import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.concurrent.Callable;

@SuppressWarnings("WeakerAccess")
@Command(
        name = "uppend",
        description = "An append-only, key-multivalue store",
        synopsisHeading = "%nUsage: ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n",
        commandListHeading = "%nCommands:%n",
        descriptionHeading = "%n",
        footerHeading = "%n",
        subcommands = {
                CommandBenchmark.class,
                CommandVersion.class,
                CommandFileStoreBenchmark.class
        }
)
public class Cli implements Callable<Void> {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Override
    public Void call() throws Exception {
        CommandLine.usage(this, System.err);
        return null;
    }

    public static void main(String... args) throws Exception {
        CommandLine cmd = new CommandLine(new Cli());
        cmd.registerConverter(Path.class, (p) -> Paths.get(p));
        // TODO how to redirect to errStream?
        try {
            cmd.parseWithHandler(new RunLast(), args);
        } catch (ExecutionException e) {
            log.error("Command failed!", e);
            throw e;
        }
    }
}
