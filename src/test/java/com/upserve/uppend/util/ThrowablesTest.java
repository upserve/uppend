package com.upserve.uppend.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class ThrowablesTest {
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testGetRootCause() {
        Throwable a = new Exception("a");
        assertEquals(a, Throwables.getRootCause(a));

        Throwable b = new Exception("b", a);
        assertEquals(a, Throwables.getRootCause(b));
    }
}
