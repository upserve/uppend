package com.upserve.uppend.cli;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class CliTest {
    private static final PrintStream origErr = System.err;
    private final ByteArrayOutputStream newErrBytes = new ByteArrayOutputStream();
    private final PrintStream newErr = new PrintStream(newErrBytes);
    private String err;

    private static final PrintStream origOut = System.out;
    private final ByteArrayOutputStream newOutBytes = new ByteArrayOutputStream();
    private final PrintStream newOut = new PrintStream(newOutBytes);
    private String out;

    @Before
    public void setUp() throws IOException {
        SafeDeleting.removeDirectory(Paths.get("build/test/cli"));
        syncStreams();
        err = "";
        out = "";

        System.setErr(newErr);
        System.setOut(newOut);
    }

    @After
    public void tearDown() throws IOException {
        System.setErr(origErr);
        System.setOut(origOut);
        SafeDeleting.removeDirectory(Paths.get("build/test/cli"));
    }

    @Test
    public void testUsage() throws Exception {
        Cli.main();
        syncStreams();
        assertEquals("", out);
        assertTrue(err.contains("Usage: uppend"));
    }

    @Test
    public void testVersion() throws Exception {
        Cli.main("version", "--verbose");
        syncStreams();
        assertTrue(out.startsWith("Uppend version"));
        assertEquals("", err);
    }

    @Test
    public void testBenchmark() throws Exception {
        Cli.main("benchmark", "-s", "small", "-b", "small", "build/test/cli/bench");
        syncStreams();
        assertTrue("expected benchmark output to contain '[benchmark is done]', but got: " + out, out.contains("[benchmark is done]"));
        assertEquals("", err);
    }

    @Test
    public void testFileStoreBenchmark() throws Exception {
        Cli.main("filestore", "-s", "small", "build/test/cli/filestore");
        syncStreams();
        assertEquals("", err);
        assertTrue("expected benchmark output to contain '[done!]', but got: " + out, out.contains("[done!]"));
    }

    @Test
    public void testBenchmarkWide() throws Exception {
        Cli.main("benchmark", "-s", "nano", "-c", "wide", "-b", "small", "build/test/cli/bench");
        syncStreams();
        assertTrue("expected benchmark output to contain '[benchmark is done]', but got: " + out, out.contains("[benchmark is done]"));
        assertEquals("", err);
    }

    @Test
    public void testBenchmarkReadWrite() throws Exception {
        Cli.main("benchmark", "-s", "nano", "-m", "readwrite", "-b", "small", "build/test/cli/bench");
        syncStreams();
        assertTrue("expected benchmark output to contain '[benchmark is done]', but got: " + out, out.contains("[benchmark is done]"));
        assertEquals("", err);
    }

    @Test
    public void testBenchmarkWriteThenRead() throws Exception {
        Cli.main("benchmark", "-s", "nano", "-m", "write", "-b", "small", "build/test/cli/bench");
        syncStreams();
        assertTrue("expected benchmark output to contain '[benchmark is done]', but got: " + out, out.contains("[benchmark is done]"));
        assertEquals("", err);
        Cli.main("benchmark", "-s", "nano", "-m", "read", "-b", "small", "build/test/cli/bench");
        syncStreams();
        assertTrue("expected benchmark output to contain '[benchmark is done]', but got: " + out, out.contains("[benchmark is done]"));
        assertEquals("", err);
    }

    private void syncStreams() {
        System.out.flush();
        newOut.flush();
        out = newOutBytes.toString();
        newOutBytes.reset();

        System.err.flush();
        newErr.flush();
        err = newErrBytes.toString();
        newErrBytes.reset();
    }
}
