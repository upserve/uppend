package com.upserve.uppend;

import org.junit.*;

public class UppendMainTest extends TestHelper.IoStreamHelper {
    @Test
    public void testMain() throws Exception {
        Uppend.main();
        assertStdErrContains("Usage: uppend");
        assertStdOut("");
    }
}
