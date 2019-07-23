package com.upserve.uppend.cli;

import com.upserve.uppend.TestHelper;
import org.junit.*;

public class CliTest extends TestHelper.IoStreamHelper {
    @Test
    public void testNoArgs() throws Exception {
        Cli.main();
        assertStdErrContains("Usage: uppend [--help] [COMMAND]");
    }

    @Test
    public void testUsage() throws Exception {
        Cli.main("--help");
        assertStdOutContains("Usage: uppend [--help] [COMMAND]");
        assertStdOutContains("benchmark  Run store benchmark");
        assertStdOutContains("version    Print version information");
        assertStdOutContains("filestore  Run file store benchmark");
    }

    @Test
    public void testBadCommand() throws Exception {
        Cli.main("foobar");
        assertStdErrContains("Unmatched argument at index 0: 'foobar'");
        assertStdErrContains("Did you mean:");
    }

    @Test
    public void testBadArgument() throws Exception {
        Cli.main("--foobar");
        assertStdErrContains("Unknown option: '--foobar'");
        assertStdErrContains("Usage: uppend [--help] [COMMAND]");
    }

    @Test
    public void testExecutionException() throws Exception {
        // Assert that an exception that occurs in method execution gets presented nicely
        Cli.main("filestore", "-n", "-1", "build/test/cli");
        assertStdErrContains("java.lang.IllegalArgumentException: virtualFiles must be greater than 0 in file");
    }
}
