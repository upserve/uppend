package com.upserve.uppend.lookup;

import com.upserve.uppend.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class LookupAppendBufferTest {
    private final Path path = Paths.get("build/test/long-lookup-test");

    private LookupAppendBuffer instance;
    private LongLookup longLookup;
    private BlockedLongs blockedLongs;

    @Before
    public void init() throws IOException {
        SafeDeleting.removeDirectory(path);

        longLookup = new LongLookup(path, 32, 0);
        blockedLongs = new BlockedLongs(path.resolve("blocks"), 127);

        instance = new LookupAppendBuffer(longLookup, blockedLongs, 24, 0, Optional.empty());
    }

    @After
    public void cleanup() throws Exception {
        instance.close();
        longLookup.close();
        blockedLongs.close();
        SafeDeleting.removeDirectory(path);
    }

    @Test
    public void testAppend() throws InterruptedException, ExecutionException {
        instance.bufferedAppend("partition1", "key", 15);
        assertEquals(1, instance.bufferCount());

        instance.bufferedAppend("partition1", "key", 72);
        assertEquals(1, instance.bufferCount());

        instance.bufferedAppend("partition2", "key", 16);
        assertEquals(2, instance.bufferCount());

        instance.flush();

        long lookup;
        lookup = longLookup.get("partition1","key");
        assertArrayEquals(new long[]{15, 72}, blockedLongs.values(lookup).toArray());

        instance.bufferedAppend("partition2", "key", 29);
        assertEquals(2, instance.bufferCount());

        // New data is not yet written
        lookup = longLookup.get("partition2","key");
        assertArrayEquals(new long[]{16}, blockedLongs.values(lookup).toArray());
    }

    @Test
    public void fixedSizeFlush() throws ExecutionException, InterruptedException {
        // Test a hot key flushing many times
        IntStream.range(0, 24*50)
                .forEach(val -> instance.bufferedAppend("partition3", "key", val));

        instance.getTasks().iterator().forEachRemaining(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                fail("Buffered write was interrupted");
            } catch (ExecutionException e) {
                fail("Buffered write cause an execution exception");
            }
        });

        long lookup = longLookup.get("partition3","key");
        assertEquals(24*50, blockedLongs.values(lookup).count());
    }

    @Test
    public void testClose(){
        instance.bufferedAppend("partition1", "key", 15);
        assertEquals(1, instance.bufferCount());

        instance.bufferedAppend("partition1", "key", 72);
        assertEquals(1, instance.bufferCount());

        instance.bufferedAppend("partition2", "key", 16);
        assertEquals(2, instance.bufferCount());

        instance.close();

        long lookup;
        lookup = longLookup.get("partition1","key");
        assertArrayEquals(new long[]{15, 72}, blockedLongs.values(lookup).toArray());

        lookup = longLookup.get("partition2","key");
        assertArrayEquals(new long[]{16}, blockedLongs.values(lookup).toArray());


        Exception expected = null;
        try {
            instance.bufferedAppend("partition2", "key", 16);
        } catch (RuntimeException e) {
            expected = e;
        }
        assertNotNull("Should have failed to write to a closed buffered appender",expected);
    }


    @Test
    public void testClear(){
        instance.bufferedAppend("partition1", "key", 15);
        assertEquals(1, instance.bufferCount());

        instance.flush();
        long lookup;
        lookup = longLookup.get("partition1","key");
        assertArrayEquals(new long[]{15}, blockedLongs.values(lookup).toArray());

        instance.clearLock();
        Exception expected = null;
        try {
            instance.bufferedAppend("partition1", "key", 15);
        }catch (RuntimeException e) {
            expected = e;
        }
        assertNotNull("Should have failed to write to a closed buffered appender", expected);

        blockedLongs.clear();
        longLookup.clear();
        instance.unlock();

        assertEquals(-1, longLookup.get("partition1","key"));

        instance.bufferedAppend("partition1", "key", 16);
        assertEquals(1, instance.bufferCount());

        instance.flush();
        lookup = longLookup.get("partition1","key");
        assertArrayEquals(new long[]{16}, blockedLongs.values(lookup).toArray());
    }
}
