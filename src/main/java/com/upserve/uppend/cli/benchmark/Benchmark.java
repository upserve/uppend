package com.upserve.uppend.cli.benchmark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.upserve.uppend.*;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;

import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.*;

public class Benchmark {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private BenchmarkWriter writer;
    private BenchmarkReader reader;

    private final Random random = new Random();

    private long range;
    private int count;
    private int maxPartitions;
    private int maxKeys;
    private int sleep = 0;

    private final MetricRegistry metrics;
    private final AppendOnlyStore testInstance;

    private volatile boolean isDone = false;

    public Benchmark(BenchmarkMode mode, Path path, int maxPartitions, int maxKeys, int count, int hashSize, int cachesize, int flushDelaySeconds, int buffered) {

        this.count = count;
        this.maxPartitions = maxPartitions; // max ~ 2000
        this.maxKeys = maxKeys; // max ~ 10,000,000

        if (Files.exists(path)) {
            log.warn("Location already exists: appending to {}", path);
        }

        metrics = new MetricRegistry();

        testInstance = Uppend.store(path)
                .withLongLookupHashSize(hashSize)
                .withLongLookupWriteCacheSize(cachesize)
                .withFlushDelaySeconds(flushDelaySeconds)
                .withBufferedAppend(buffered, AutoFlusher.flushExecPool)
                .withMetrics(metrics)
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
        int length =(int) (integer % 1024);
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
            int i = 0;
            while (!isDone) {
                try {
                    Timer writeTimer = metrics.getTimers().get(WRITE_TIMER_METRIC_NAME);
                    Meter writeBytesMeter = metrics.getMeters().get(WRITE_BYTES_METER_METRIC_NAME);
                    Timer readTimer = metrics.getTimers().get(READ_TIMER_METRIC_NAME);
                    Meter readBytesMeter = metrics.getMeters().get(READ_BYTES_METER_METRIC_NAME);

                    long written = writeBytesMeter.getCount();
                    long writeCount = writeTimer.getCount();
                    long read = readBytesMeter.getCount();
                    long readCount = readTimer.getCount();

                    Thread.sleep(1000);

                    double writeRate = (writeBytesMeter.getCount() - written) / (1024.0 * 1024.0);
                    long appendsPerSecond = writeTimer.getCount() - writeCount;
                    double readRate = (readBytesMeter.getCount() - read) / (1024.0 * 1024.0);
                    long keysReadPerSecond = readTimer.getCount() - readCount;
                    double total = runtime.totalMemory() / (1024.0 * 1024.0);
                    double free = runtime.freeMemory() / (1024.0 * 1024.0);

                    log.info(String.format("Read: %7.2fmb/s %6dr/s; Write %7.2fmb/s %6da/s; Mem %7.2fmb free %7.2fmb total", readRate, keysReadPerSecond,  writeRate, appendsPerSecond, free, total));

                    i++;
                    if ((i % 10) == 0) {
                        log.info(testInstance.cacheStats().toString());
                    }

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

        AutoFlusher.flushExecPool.shutdown();

        log.info("Benchmark is All Done!");
        System.out.println("[benchmark is done]"); // used in CliTest
        isDone = true;
    }
}

