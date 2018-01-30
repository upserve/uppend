package com.upserve.uppend;

import com.upserve.uppend.util.*;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class BlockedLongsTest {
    private Path path = Paths.get("build/test/tmp/block");
    private Path posPath = path.resolveSibling(path.getFileName() + ".pos");

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(path);
        SafeDeleting.removeTempPath(posPath);
    }

    @Test
    public void testCtor() {
        new BlockedLongs(path, 1);
        new BlockedLongs(path, 10);
        new BlockedLongs(path, 100);
        new BlockedLongs(path, 1000);
    }

    @Test(expected = UncheckedIOException.class)
    public void testCtorNoPosFile() throws Exception {
        BlockedLongs block = new BlockedLongs(path, 1);
        block.close();
        Files.delete(posPath);
        Files.createDirectories(posPath);
        new BlockedLongs(path, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithNullFile() {
        new BlockedLongs(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithZeroValuesPerBlock() {
        new BlockedLongs(path, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithNegativeValuesPerBlock() {
        new BlockedLongs(path, -1);
    }

    @Test
    public void testAllocate() throws Exception {
        for (int i = 1; i <= 20; i++) {
            BlockedLongs v = new BlockedLongs(path, i);
            long pos1 = v.allocate();
            long pos2 = v.allocate();
            assertEquals(0, pos1);
            assertEquals(16 + (8 * i), pos2); // brittle
            v.clear();
        }
    }

    @Test
    public void testAppend() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        long pos1 = v.allocate();
        for (long i = 0; i < 20; i++) {
            v.append(pos1, i);
        }
        long pos2 = v.allocate();
        for (long i = 100; i < 120; i++) {
            v.append(pos2, i);
        }
        assertArrayEquals(new long[]{
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        }, v.values(pos1).toArray());
        assertArrayEquals(new long[]{
                100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119
        }, v.values(pos2).toArray());
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendAtNonStartingBlock() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        long pos1 = v.allocate();
        for (long i = 0; i < 21; i++) {
            v.append(pos1, i);
        }
        int blockSize = 16 + 10 * 8; // mirrors BlockedLongs.blockSize
        v.append(blockSize * 2, 21);
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendLastBlockHasANext() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        assertEquals(0, v.allocate());
        v.append(0, 0);
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer longBuf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
            longBuf.putLong(-1);
            longBuf.flip();
            chan.write(longBuf, 0);
        }
        v.append(0, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendTooHighNumValues() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        long pos1 = v.allocate();
        try (FileChannel chan = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer longBuf = ThreadLocalByteBuffers.LOCAL_LONG_BUFFER.get();
            longBuf.putLong(20);
            longBuf.flip();
            chan.write(longBuf, 0);
        }
        v.append(pos1, 0);
    }

    @Test
    public void testLastValue() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 4);
        long pos = v.allocate();
        for (long i = 0; i < 257; i++) {
            v.append(pos, i);
        }
        assertEquals(256, v.lastValue(pos));
    }

    @Test
    public void testFlushAndCloseTwice() throws Exception {
        BlockedLongs block = new BlockedLongs(path, 1);
        block.flush();
        block.flush();
        block.close();
        block.close();
    }

    @Test
    public void blockedLongBeating() throws Exception {
        int VALS_PER_BLOCK = 3;
        int TEST_POSITIONS = 20_000;
        long TEST_APPENDS = 200_000;
        ConcurrentHashMap<Long, ArrayList<Long>> testData = new ConcurrentHashMap<>();
        Supplier<Long> valueSupplier = () -> ThreadLocalRandom.current().nextLong(0, 1_000_000);
        BlockedLongs block;
        LongStream positions;

        block = new BlockedLongs(path, VALS_PER_BLOCK);
        positions = new Random().longs(0, TEST_POSITIONS).limit(TEST_APPENDS).parallel();
        blockBeating(block, valueSupplier, positions, testData);
        block.close();

        block = new BlockedLongs(path, VALS_PER_BLOCK);
        positions = new Random().longs(0, TEST_POSITIONS * 2).limit(TEST_APPENDS).parallel();
        blockBeating(block, valueSupplier, positions, testData);
        block.close();

        block = new BlockedLongs(path, VALS_PER_BLOCK);
        positions = new Random().longs(0, TEST_POSITIONS * 3).limit(TEST_APPENDS).parallel();
        blockBeating(block, valueSupplier, positions, testData);
        block.close();

        block = new BlockedLongs(path, VALS_PER_BLOCK);
        positions = new Random().longs(0, TEST_POSITIONS * 4).limit(TEST_APPENDS).parallel();
        blockBeating(block, valueSupplier, positions, testData);
        block.close();

        assertEquals(TEST_APPENDS * 4, testData.values().stream().mapToLong(List::size).sum());

        long expectedBlocks = testData.values().stream().mapToLong(vals -> (vals.size() + VALS_PER_BLOCK -1) / VALS_PER_BLOCK ).sum();
        long actualBlocks = block.size()/(16 + VALS_PER_BLOCK * 8);
        assertEquals(expectedBlocks, actualBlocks);
    }


    public void blockBeating(BlockedLongs block, Supplier<Long> valueSupplier, LongStream positions, ConcurrentHashMap<Long, ArrayList<Long>> testData) {
        positions.forEach(pos -> {
            long value = valueSupplier.get();

            List<Long> exists = testData.computeIfPresent(pos, (posKey, list) -> {
                list.add(value);
                block.append(posKey, value);
                return list;
            });

            if (exists == null){
                long newPos = block.allocate();
                ArrayList<Long> values = new ArrayList<>();
                values.add(value);
                testData.put(newPos, values);
                block.append(newPos, value);

                assertEquals(value, block.lastValue(newPos));
            }

            if ((pos % 10_000) == 0) {
                block.flush();
            }
        });
        block.flush();

        testData.entrySet().parallelStream().forEach(entry -> {
            assertArrayEquals(
                    entry.getValue().stream().sorted().mapToLong(value -> value).toArray(),
                    block.values(entry.getKey()).sorted().toArray());
        });
    }


}
