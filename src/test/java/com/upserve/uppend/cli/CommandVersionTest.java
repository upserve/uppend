package com.upserve.uppend.cli;

import com.upserve.uppend.TestHelper;
import org.junit.*;
import picocli.CommandLine;

public class CommandVersionTest extends TestHelper.IoStreamHelper {

    @Test
    public void testVersion() throws Exception {
        new CommandLine(new CommandVersion()).execute();
        assertStdOutContains("test-version");
    }

    @Test
    public void testVersionHelp() throws Exception {
        new CommandLine(new CommandVersion()).execute("--help");
        assertStdOutContains("Usage: uppend version [-v] [--help]");
    }

    @Test
    public void testVersionVerbose() throws Exception {
        new CommandLine(new CommandVersion()).execute("-v");
        assertStdOutContains("Uppend version test-version");
    }
}
