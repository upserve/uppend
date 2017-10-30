package com.upserve.uppend.cli;

import com.upserve.uppend.Uppend;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@SuppressWarnings("WeakerAccess")
@Command(
        name = "version",
        description = "Print version information",
        synopsisHeading = "%nUsage: uppend ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n",
        commandListHeading = "%nCommands:%n",
        descriptionHeading = "%n",
        footerHeading = "%n"
)
public class CommandVersion implements Callable<Void> {
    @SuppressWarnings("unused")
    @Option(names = "--help", usageHelp = true, description = "Print usage")
    boolean help;

    @Option(names = {"-v", "--verbose"}, description = "Show verbose output")
    boolean verbose;

    @Override
    public Void call() throws Exception {
        if (verbose) {
            System.out.printf("Uppend version %s\n", Uppend.VERSION);
        } else {
            System.out.printf("%s\n", Uppend.VERSION);
        }
        return null;
    }
}
