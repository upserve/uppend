package com.upserve.uppend.cli;

import com.upserve.uppend.TestHelper;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.assertEquals;

public class FileStoreBenchmarkTest extends TestHelper.IoStreamHelper {

    CommandFileStoreBenchmark commandBenchmark;
    CommandLine commandLine;
    
    @Before
    public void setUpDir() throws IOException {
        commandBenchmark = new CommandFileStoreBenchmark();
        commandLine = new CommandLine(commandBenchmark).registerConverter(Path.class, (p) -> Paths.get(p));
        SafeDeleting.removeDirectory(Paths.get("build/test/cli"));
    }

    @After
    public void tearDownDir() throws IOException {
        SafeDeleting.removeDirectory(Paths.get("build/test/cli"));
    }

    @Test
    public void tesUsage() {
        commandLine.execute("--help");
        assertStdOutContains("Usage: uppend filestore [--help] [-b=<bufferSize>] [-n=<nfiles>]");
        assertStdOutContains("[-p=<pageSize>] [-s=<size>] <path>");
    }

    @Test
    public void testMissingRequiredPath() {
        commandLine.execute("-b", "small");
        assertStdErrContains("Missing required parameter: <path>");
    }

    @Test
    public void testBadArgument() {
        commandLine.execute("--badArg", "pretendPath");
        assertStdErrContains("Unknown option: '--badArg'");
    }

    @Test
    public void testSmall() {
        commandLine.execute("-b", "small", "-n", "4", "-s", "small", "build/test/cli");
        assertStdOutContains("[All Done!]");

        assertEquals(1000000, commandBenchmark.getStats().getCount());
    }
}
