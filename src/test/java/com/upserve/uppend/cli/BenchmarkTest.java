package com.upserve.uppend.cli;

import com.upserve.uppend.TestHelper;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.assertEquals;

public class BenchmarkTest extends TestHelper.IoStreamHelper {

    CommandBenchmark commandBenchmark;
    CommandLine commandLine;

    @Before
    public void setUpDir() throws IOException {
        commandBenchmark = new CommandBenchmark();
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
        assertStdOutContains("Usage: uppend benchmark [-k] [--help] [-b=<bufferSize>] [-c=<benchmarkCase>]");
        assertStdOutContains("[-m=<mode>] [-s=<size>] <path>");
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
    public void testBenchmark() {
        commandLine.execute("-s", "small", "-b", "small", "-k", "build/test/cli/bench");
        assertEquals(1000000L, commandBenchmark.benchmark.writerStats().getCount());
    }

    @Test
    public void testBenchmarkWide() {
        commandLine.execute("-s", "nano", "-c", "wide", "-b", "small", "build/test/cli/bench");
        assertEquals(20000L, commandBenchmark.benchmark.writerStats().getCount());
    }

    @Test
    public void testBenchmarkReadWrite() {
        commandLine.execute("-s", "nano", "-m", "readwrite", "-b", "small", "build/test/cli/bench");
        assertEquals(10000L, commandBenchmark.benchmark.writerStats().getCount());
        assertEquals(10000L, commandBenchmark.benchmark.readerStats().getCount());
    }

    @Test
    public void testBenchmarkWriteThenRead() {
        commandLine.execute("-s", "nano", "-m", "write", "-b", "small", "build/test/cli/bench");
        assertEquals(10000L, commandBenchmark.benchmark.writerStats().getCount());

        commandLine.execute("-s", "nano", "-m", "read", "-b", "small", "build/test/cli/bench");
        assertEquals(10000L, commandBenchmark.benchmark.readerStats().getCount());
    }
}
