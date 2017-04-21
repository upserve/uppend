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
    public void validate_success() {
        Partition.validate("2016-12-01");
    }

    @Test
    public void validate_exception() {
        String partition = "2016-12**01";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Partition must be non-empty, consist of valid Java identifier characters and /, and have zero-width parts surrounded by /: " + partition);
        Partition.validate(partition);
    }

    @Test
    public void testSpecialCharacters() {
        assertTrue(Partition.verifier("pre-fix"));
        assertTrue(Partition.verifier("pre/fix"));
        assertTrue(Partition.verifier("pre$fix"));
        assertTrue(Partition.verifier("pre_fix"));

        assertFalse(Partition.verifier("pre!fix"));
        assertFalse(Partition.verifier("pre@fix"));
        assertFalse(Partition.verifier("pre#fix"));
        assertFalse(Partition.verifier("pre%fix"));
        assertFalse(Partition.verifier("pre^fix"));
        assertFalse(Partition.verifier("pre&fix"));
        assertFalse(Partition.verifier("pre*fix"));
        assertFalse(Partition.verifier("pre(fix"));
        assertFalse(Partition.verifier("pre)fix"));
        assertFalse(Partition.verifier("pre+fix"));
        assertFalse(Partition.verifier("pre=fix"));
        assertFalse(Partition.verifier("pre:fix"));
        assertFalse(Partition.verifier("pre'fix"));
        assertFalse(Partition.verifier("pre;fix"));
        assertFalse(Partition.verifier("pre\"fix"));
        assertFalse(Partition.verifier("pre{fix"));
        assertFalse(Partition.verifier("pre}fix"));
        assertFalse(Partition.verifier("pre\\fix"));
        assertFalse(Partition.verifier("pre|fix"));
        assertFalse(Partition.verifier("pre.fix"));
        assertFalse(Partition.verifier("pre,fix"));
        assertFalse(Partition.verifier("pre?fix"));
    }

    @Test
    public void testEmptyOrEmptyPart() {
        assertFalse(Partition.verifier(""));
        assertFalse(Partition.verifier("foo//bar"));
    }

    @Test
    public void testSpecialSlashes() {
        assertTrue(Partition.verifier("part/it/ion"));

        assertFalse(Partition.verifier("/partition"));
        assertFalse(Partition.verifier("partition/"));
        assertFalse(Partition.verifier("/"));
        assertFalse(Partition.verifier("//"));
    }

    @Test
    public void testJustSpecial() {
        assertTrue(Partition.verifier("$"));
        assertTrue(Partition.verifier("_"));

        assertFalse(Partition.verifier("/"));
        assertFalse(Partition.verifier("-"));
    }
}
