package com.upserve.uppend.cli.benchmark;

import com.upserve.uppend.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.*;
import java.util.*;


@Slf4j
public class Benchmark {
    private BenchmarkWriter writer;
    private BenchmarkReader reader;

    private final Random random = new Random();

    private long range;
    private int count;
    private int maxPartitions;
    private int maxKeys;
    private int sleep = 0;

    private final AppendOnlyStore testInstance;

    public Benchmark(BenchmarkMode mode, Path path, int maxPartitions, int maxKeys, int count, int hashSize, int cachesize, int flushDelaySeconds) {

        this.count = count;
        this.maxPartitions = maxPartitions; // max ~ 2000
        this.maxKeys = maxKeys; // max ~ 10,000,000

        if (Files.exists(path)) {
            log.warn("Location already exists: appending to {}", path);
        }

        testInstance = Uppend.store(path)
                .withLongLookupHashSize(hashSize)
                .withLongLookupWriteCacheSize(cachesize)
                .withFlushDelaySeconds(flushDelaySeconds)
                .build();


        range = (long) maxPartitions * (long) maxKeys * 199;


        switch (mode) {
            case readwrite:
                writer = simpleWriter();
                reader = simpleReader();
                sleep = 31;
                break;

            case read:
                writer = BenchmarkWriter.noop();
                reader = simpleReader();
                break;

            case write:
                writer = simpleWriter();
                reader = BenchmarkReader.noop();
                break;
            default:
                throw new RuntimeException("Unknown mode: " + mode);
        }

    }

    private BenchmarkWriter simpleWriter() {
        return new BenchmarkWriter(
                random.longs(count, 0, range).parallel(),
                longInt -> {
                    byte[] myBytes = bytes(longInt);
                    testInstance.append(partition(longInt, maxPartitions), key(longInt/maxPartitions, maxKeys), myBytes);
                    return myBytes.length;
                }
        );
    }

    private BenchmarkReader simpleReader() {
        return new BenchmarkReader(
                random.longs(count, 0, range).parallel(),
                longInt -> testInstance.read(partition(longInt, maxPartitions), key(longInt/maxPartitions, maxKeys))
                            .mapToInt(theseBytes -> theseBytes.length)
                            .sum()
        );
    }

    public static String key(long integer, int maxKeys) {
        return String.format("%08X", integer % maxKeys);
    }

    private static String partition(long integer, int maxPartitions) {
        return String.format("_%04X", integer % maxPartitions);
    }

    public static byte[] bytes(long integer) {
        int length =(int) (integer % 65536);
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 0);
        return bytes;
    }

    public void run() throws InterruptedException {
        log.info("Running Performance test with {} partitions, {} keys and {} count", maxPartitions, maxKeys, count);

        Thread writerThread = new Thread(writer);
        Thread readerThread = new Thread(reader);

        Thread watcher = new Thread(() -> {
            Runtime runtime = Runtime.getRuntime();
            while (true) {
                try {
                    long written = writer.bytesWritten.get();
                    long writeCount = writer.writeCount.get();
                    long read = reader.bytesRead.get();
                    long readCount = reader.readCount.get();
                    Thread.sleep(1000);
                    double writeRate = (writer.bytesWritten.get() - written) / (1024.0 * 1024.0);
                    long appendsPerSecond = writer.writeCount.get() - writeCount;
                    double readRate = (reader.bytesRead.get() - read) / (1024.0 * 1024.0);
                    long keysReadPerSecond = reader.readCount.get() - readCount;
                    double total = runtime.totalMemory() / (1024.0 * 1024.0);
                    double free = runtime.freeMemory() / (1024.0 * 1024.0);
                    log.info(String.format("Read: %7.2fmb/s %6dr/s; Write %7.2fmb/s %6da/s; Mem %7.2fmb free %7.2fmb total", readRate, keysReadPerSecond,  writeRate, appendsPerSecond, free, total));
                } catch (InterruptedException e) {
                    log.info("Interrupted - Stopping...");
                    break;
                }

            }
        });

        writerThread.start();
        Thread.sleep(sleep * 1000); // give the writer a head start...
        readerThread.start();
        watcher.start();

        writerThread.join();
        readerThread.join();


        watcher.join(1500);

        try {
            testInstance.close();
        } catch (Exception e) {
            throw new RuntimeException("error closing test uppend store", e);
        }

        log.info("Benchmark is All Done!");
    }
}

