package com.upserve.uppend.lookup;

import org.junit.Test;

import static org.junit.Assert.*;

public class LookupMetadataTest {
    @Test
    public void testSearchMidpointPercentageFunction() {
        assertEquals(48, LookupMetadata.searchMidpointPercentage("aa", "zyzzyva", "middle"));
    }

    @Test
    public void testSearchMidpointPercentageFunctionEdgeCases() {
        assertEquals(50, LookupMetadata.searchMidpointPercentage("zyzzyva", "aa", "middle"));
        assertEquals(50, LookupMetadata.searchMidpointPercentage("aa", "be", "oi"));
    }
}
