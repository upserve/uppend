package com.upserve.uppend.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class PartitionTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void validate_success(){
        String partition = "2016-12-01";
        Partition.validate(partition);
    }

    @Test
    public void validate_exception(){
        String partition = "2016-12**01";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Partition must be non-empty, consist of valid Java identifier characters and /, and have zero-width parts surrounded by /: " + partition);
        Partition.validate(partition);
    }

    @Test
    public void verifier(){
        assertTrue(Partition.verifier("2016-12-01"));
        assertTrue(Partition.verifier("2016/12/01"));
        assertFalse(Partition.verifier("2016//01"));
        assertFalse(Partition.verifier("2016/*01"));
        assertFalse(Partition.verifier(""));
    }
}
