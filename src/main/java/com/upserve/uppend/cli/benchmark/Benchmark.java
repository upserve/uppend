package com.upserve.uppend.cli.benchmark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.*;
import com.upserve.uppend.AppendOnlyStoreBuilder;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.*;

public class Benchmark {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Runnable writer;
    private Runnable reader;

    private final Random random = new Random();

    private BenchmarkMode mode;

    private long range;
    private int count;
    private int maxPartitions;
    private int maxKeys;
    private int sleep = 0;

    private final MetricRegistry metrics;
    private final AppendOnlyStore testInstance;

    private volatile boolean isDone = false;

    public Benchmark(BenchmarkMode mode, Path path, int maxPartitions, int maxKeys, int count, int hashSize, int keyCachesize, int metadataCacheSize, int openFileCacheSize, int blobPageCacheSize, int keyPageCacheSize, int flushDelaySeconds) {
        this.mode = mode;

        this.count = count;
        this.maxPartitions = maxPartitions; // max ~ 2000
        this.maxKeys = maxKeys; // max ~ 10,000,000

        if (Files.exists(path)) {
            log.warn("Location already exists: appending to {}", path);
        }

        metrics = new MetricRegistry();

        AppendOnlyStoreBuilder builder = Uppend.store(path)
                .withBlobsPerBlock(14)
                .withLongLookupHashSize(hashSize)
                .withIntialFileCacheSize(openFileCacheSize)
                .withMaximumFileCacheSize(openFileCacheSize)
                .withInitialLookupKeyCacheSize(keyCachesize)
                .withMaximumLookupKeyCacheWeight(keyCachesize * 256) // based on key size
                .withInitialBlobCacheSize(blobPageCacheSize)
                .withMaximumBlobCacheSize(blobPageCacheSize)
                .withInitialLookupPageCacheSize(keyPageCacheSize)
                .withMaximumLookupPageCacheSize(keyPageCacheSize)
                .withInitialMetaDataCacheSize(metadataCacheSize)
                .withMaximumMetaDataCacheWeight(metadataCacheSize * (maxKeys / hashSize))
                .withFlushDelaySeconds(flushDelaySeconds)
                .withStoreMetrics(metrics)
                .withCacheMetrics();


        log.info(builder.toString());

        range = (long) maxPartitions * (long) maxKeys * 199;

        switch (mode) {
            case readwrite:
                testInstance = builder.build(false);
                writer = simpleWriter();
                reader = simpleReader();
                sleep = 31;

                break;

            case read:
                testInstance = builder.build(true);
                writer = BenchmarkWriter.noop();
                reader = simpleReader();
                break;

            case write:
                testInstance = builder.build(false);
                writer = simpleWriter();
                reader = BenchmarkReader.noop();
                break;
            case scan:
                testInstance = builder.build(true);
                writer = BenchmarkWriter.noop();
                reader = scanReader(testInstance);
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

    private Runnable scanReader(AppendOnlyStore appendOnlyStore) {
        return () -> {
            LongStream
                    .range(0, maxPartitions)
                    .parallel()
                    .mapToObj(val -> partition(val, maxPartitions))
                    .flatMapToLong(parition -> {
                        log.info("reading partition: {}", parition);
                        return appendOnlyStore.scan(parition).parallel().mapToLong(entry -> entry.getValue().count());
                    })
                    .count();
        };
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

    private TimerTask watcherTimer() {

        final Timer writeTimer = metrics.getTimers().get(testInstance.getName() + "." + WRITE_TIMER_METRIC_NAME);
        final Meter writeBytesMeter = metrics.getMeters().get(testInstance.getName() + "." + WRITE_BYTES_METER_METRIC_NAME);

        final Meter readBytesMeter;
        final Supplier<Long>  readCounter;

        if (mode.equals(BenchmarkMode.scan)) {
            readCounter = () -> metrics.getMeters().get(testInstance.getName() + "." + SCAN_KEYS_METER_METRIC_NAME).getCount();
            readBytesMeter = metrics.getMeters().get(testInstance.getName() + "." + SCAN_BYTES_METER_METRIC_NAME);
        } else {
            readCounter = () -> metrics.getTimers().get(testInstance.getName() + "." + READ_TIMER_METRIC_NAME).getCount();
            readBytesMeter = metrics.getMeters().get(testInstance.getName() + "." + READ_BYTES_METER_METRIC_NAME);
        }


        final Runtime runtime = Runtime.getRuntime();

        AtomicLong tic = new AtomicLong(System.currentTimeMillis());
        AtomicLong written = new AtomicLong(writeBytesMeter.getCount());
        AtomicLong writeCount = new AtomicLong(writeTimer.getCount());
        AtomicLong read = new AtomicLong(readBytesMeter.getCount());
        AtomicLong readCount = new AtomicLong(readCounter.get());

        AtomicReference<CacheStats> blobPageCacheStats = new AtomicReference<CacheStats>(testInstance.getBlobPageCacheStats());
        AtomicReference<CacheStats> keyPageCacheStats = new AtomicReference<CacheStats>(testInstance.getKeyPageCacheStats());
        AtomicReference<CacheStats> fileCacheStats = new AtomicReference<CacheStats>(testInstance.getFileCacheStats());
        AtomicReference<CacheStats> lookupKeyCacheStats = new AtomicReference<CacheStats>(testInstance.getLookupKeyCacheStats());
        AtomicReference<CacheStats> metadataCacheStats = new AtomicReference<CacheStats>(testInstance.getMetadataCacheStats());

        return new TimerTask() {
            @Override
            public void run() {
                long val;
                CacheStats stats;
                try {
                    val = System.currentTimeMillis();
                    double elapsed = (val - tic.getAndSet(val)) / 1000D;

                    val = writeBytesMeter.getCount();
                    double writeRate = (val - written.getAndSet(val)) / (1024.0 * 1024.0) / elapsed;

                    val = writeTimer.getCount();
                    double appendsPerSecond = (val - writeCount.getAndSet(val)) / elapsed;

                    val = readBytesMeter.getCount();
                    double readRate = (val - read.getAndSet(val)) / (1024.0 * 1024.0) / elapsed;

                    val = readCounter.get();
                    double keysReadPerSecond = (val - readCount.getAndSet(val)) / elapsed;

                    double total = runtime.totalMemory() / (1024.0 * 1024.0);
                    double free = runtime.freeMemory() / (1024.0 * 1024.0);

                    log.info(String.format("Read: %7.2fmb/s %7.2fr/s; Write %7.2fmb/s %7.2fa/s; Mem %7.2fmb free %7.2fmb total", readRate, keysReadPerSecond, writeRate, appendsPerSecond, free, total));

                    stats = testInstance.getFileCacheStats();
                    log.info("File Cache: {}", stats.minus(fileCacheStats.getAndSet(stats)));

                    stats = testInstance.getBlobPageCacheStats();
                    log.info("Blob Page Cache: {}", stats.minus(blobPageCacheStats.getAndSet(stats)));

                    stats = testInstance.getKeyPageCacheStats();
                    log.info("Key Page Cache: {}", stats.minus(keyPageCacheStats.getAndSet(stats)));

                    stats = testInstance.getLookupKeyCacheStats();
                    log.info("Lookup Key Cache: {}", stats.minus(lookupKeyCacheStats.getAndSet(stats)));

                    stats = testInstance.getMetadataCacheStats();
                    log.info("Metadata Cache: {}", stats.minus(metadataCacheStats.getAndSet(stats)));

                } catch (Exception e) {
                    log.info("logTimer failed with ", e);
                }
            }
        };
    }
    public void run() throws InterruptedException {
        log.info("Running Performance test with {} partitions, {} keys and {} count", maxPartitions, maxKeys, count);

        Thread writerThread = new Thread(writer);
        Thread readerThread = new Thread(reader);

        writerThread.start();
        Thread.sleep(sleep * 1000); // give the writer a head start...
        readerThread.start();
        Thread.sleep(100);

        java.util.Timer watcherTimer = new java.util.Timer();
        watcherTimer.schedule(watcherTimer(), 5000, 5000);

        writerThread.join();
        readerThread.join();

        log.info("Threads joined - cleanup and shutdown!");

        testInstance.trim();

        log.info("Finished trim - close and shutdown");

        watcherTimer.cancel();

        try {
            testInstance.close();
        } catch (Exception e) {
            throw new RuntimeException("error closing test uppend store", e);
        }

        log.info("Benchmark is All Done!");
        System.out.println("[benchmark is done]"); // used in CliTest
        isDone = true;
    }
}

