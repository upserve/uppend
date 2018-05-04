package com.upserve.uppend;

import org.junit.*;

import java.io.*;

import static org.junit.Assert.assertTrue;

public class UppendMainTest {
    private final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errStream = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outStream));
        System.setErr(new PrintStream(errStream));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(null);
        System.setErr(null);
    }

    @Test
    public void testMain() throws Exception {
        Uppend.main("--help");
        String errStr = errStream.toString();
        assertTrue("didn't find expected 'Usage: uppend' in main stderr output: " + errStr, errStr.contains("Usage: uppend"));
    }
}
