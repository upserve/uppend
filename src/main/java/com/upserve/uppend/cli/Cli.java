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
        header = {
            "@|green ooooo     ooo                                                   .o8 |@",
            "@|green `888'     `8'                                                   888 |@",
            "@|green  888       8  oo.ooooo.  oo.ooooo.   .ooooo.  ooo. .oo.    .oooo888 |@",
            "@|green  888       8   888' `88b  888' `88b d88' `88b `888P'Y88b  d88' `888 |@",
            "@|green  888       8   888   888  888   888 888ooo888  888   888  888   888 |@",
            "@|green  `88.    .8'   888   888  888   888 888    .o  888   888  888   888 |@",
            "@|green    `YbodP'     888bod8P'  888bod8P' `Y8bod8P' o888o o888o `Y8bod88P |@",
            "@|green                888        888                                       |@",
            "@|green               o888o      o888o                                      |@",
        },
        description = "Uppend is an append-only, key-multivalue store",
        synopsisHeading = "%nUsage: ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n",
        commandListHeading = "%nCommands:%n",
        descriptionHeading = "%n",
        footerHeading = "%n",
        subcommands = {
                CommandVersion.class,
                CommandBenchmark.class,
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
        new CommandLine(new Cli())
            .registerConverter(Path.class, (p) -> Paths.get(p))
            .execute(args);
    }
}
