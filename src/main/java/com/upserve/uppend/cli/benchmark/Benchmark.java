package com.upserve.uppend.cli.benchmark;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.upserve.uppend.*;
import com.upserve.uppend.metrics.*;
import com.upserve.uppend.metrics.LookupDataMetrics;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import static com.upserve.uppend.AutoFlusher.forkJoinPoolFunction;
import static com.upserve.uppend.cli.CommandBenchmark.ROOT_NAME;
import static com.upserve.uppend.cli.CommandBenchmark.STORE_NAME;
import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.*;

public class Benchmark {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private BenchmarkRunnable writer;
    private BenchmarkRunnable reader;

    private BenchmarkMode mode;

    private final MetricRegistry metrics;
    private long range;
    private long count;
    private int sleep = 0;
    private int partitionCount;
    private int hashCount;

    private final AppendOnlyStore testInstance;

    private final ForkJoinPool writerPool;
    private final ForkJoinPool readerPool;

    AtomicReference<LookupDataMetrics> lookupDataMetricsReference;
    AtomicReference<BlockedLongMetrics> blockedLongMetricsReference;
    AtomicReference<BlobStoreMetrics> blobStoreMetricsReference;
    AtomicReference<LongBlobStoreMetrics> longBlobStoreMetricsReference;
    AtomicReference<MutableBlobStoreMetrics> mutableBlobStoreMetricsReference;

    public LongSummaryStatistics writerStats() {
        return writer.getStats();
    }

    public LongSummaryStatistics readerStats() {
        return reader.getStats();
    }

    public Benchmark(BenchmarkMode mode, AppendOnlyStoreBuilder builder, long range, long count) {
        this.mode = mode;

        this.count = count;
        this.range = range;

        partitionCount = builder.getPartitionCount();
        hashCount = builder.getLookupHashCount();

        writerPool = forkJoinPoolFunction.apply("benchmark-writer");
        readerPool = forkJoinPoolFunction.apply("benchmark-reader");

        metrics = builder.getStoreMetricsRegistry();

        log.info(builder.toString());

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
                reader = new ScanReader(testInstance);
                break;
            default:
                throw new RuntimeException("Unknown mode: " + mode);
        }

        lookupDataMetricsReference = new AtomicReference<>(testInstance.getLookupDataMetrics());
        blockedLongMetricsReference = new AtomicReference<>(testInstance.getBlockedLongMetrics());
        blobStoreMetricsReference = new AtomicReference<>(testInstance.getBlobStoreMetrics());
        longBlobStoreMetricsReference = new AtomicReference<>(testInstance.getLongBlobStoreMetrics());
        mutableBlobStoreMetricsReference = new AtomicReference<>(testInstance.getMutableBlobStoreMetrics());
    }

    private BenchmarkWriter simpleWriter() {
        return new BenchmarkWriter(
                ThreadLocalRandom.current().longs(count, 0, range).parallel(),
                longInt -> {
                    byte[] myBytes = bytes(longInt);
                    String formatted = format(longInt);
                    testInstance.append(formatted, formatted, myBytes);
                    return myBytes.length;
                }
        );
    }

    private BenchmarkReader simpleReader() {
        return new BenchmarkReader(
                ThreadLocalRandom.current().longs(count, 0, range).parallel(),
                longInt -> {
                    String formatted = format(longInt);
                    return testInstance.read(formatted, formatted)
                            .mapToInt(theseBytes -> theseBytes.length)
                            .sum();
                }
        );
    }

    private class ScanReader implements BenchmarkRunnable{
        private final AppendOnlyStore appendOnlyStore;
        private LongSummaryStatistics result;

        public ScanReader(AppendOnlyStore appendOnlyStore) {
            this.appendOnlyStore = appendOnlyStore;
        }

        @Override
        public LongSummaryStatistics getStats() {
            return result;
        }


        @Override
        public void run() {
            result = appendOnlyStore.scan().mapToLong(entry -> entry.getValue().count()).summaryStatistics();
            log.info("Scanned {} entries", count);
        }
    }

    public static String format(long value) {
        return String.format("%09X", value);
    }

    public static byte[] bytes(long value) {
        int length = (int) (value % 1024);
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 123);
        return bytes;
    }

    private TimerTask watcherTimer() {

        final Timer writeTimer = metrics.timer(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, WRITE_TIMER_METRIC_NAME));
        final Meter writeBytesMeter = metrics.meter(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, WRITE_BYTES_METER_METRIC_NAME));

        final Meter readBytesMeter;
        final Supplier<Long> readCounter;

        if (mode.equals(BenchmarkMode.scan)) {
            readCounter = () -> metrics.meter(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, SCAN_KEYS_METER_METRIC_NAME)).getCount();
            readBytesMeter = metrics.meter(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, SCAN_BYTES_METER_METRIC_NAME));
        } else {
            readCounter = () -> metrics.timer(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, READ_TIMER_METRIC_NAME)).getCount();
            readBytesMeter = metrics.meter(MetricRegistry.name(ROOT_NAME, UPPEND_APPEND_STORE, STORE_NAME, READ_BYTES_METER_METRIC_NAME));
        }

        final Runtime runtime = Runtime.getRuntime();

        AtomicLong tic = new AtomicLong(System.currentTimeMillis());
        AtomicLong written = new AtomicLong(writeBytesMeter.getCount());
        AtomicLong writeCount = new AtomicLong(writeTimer.getCount());
        AtomicLong read = new AtomicLong(readBytesMeter.getCount());
        AtomicLong readCount = new AtomicLong(readCounter.get());

        return new TimerTask() {
            @Override
            public void run() {
                long val;
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

                    LookupDataMetrics lookupDataMetrics = testInstance.getLookupDataMetrics();
                    log.info(lookupDataMetrics.present(lookupDataMetricsReference.getAndSet(lookupDataMetrics)));

                    BlockedLongMetrics blockedLongMetrics = testInstance.getBlockedLongMetrics();
                    log.info(blockedLongMetrics.present(blockedLongMetricsReference.getAndSet(blockedLongMetrics)));

                    BlobStoreMetrics blobStoreMetrics = testInstance.getBlobStoreMetrics();
                    log.info(blobStoreMetrics.present(blobStoreMetricsReference.getAndSet(blobStoreMetrics)));

                    LongBlobStoreMetrics longBlobStoreMetrics = testInstance.getLongBlobStoreMetrics();
                    log.info(longBlobStoreMetrics.present(longBlobStoreMetricsReference.getAndSet(longBlobStoreMetrics)));

                    MutableBlobStoreMetrics mutableBlobStoreMetrics = testInstance.getMutableBlobStoreMetrics();
                    log.info(mutableBlobStoreMetrics.present(mutableBlobStoreMetricsReference.getAndSet(mutableBlobStoreMetrics)));

                } catch (Exception e) {
                    log.info("logTimer failed with ", e);
                }
            }
        };
    }

    public void run() throws InterruptedException, ExecutionException, IOException {
        log.info("Running Performance test with {} partitions {} hashCount, {} keys and {} count", partitionCount, hashCount, range, count);

        Future writerFuture = writerPool.submit(writer);

        Thread.sleep(sleep * 1000); // give the writer a head start...

        Future readerFuture = readerPool.submit(reader);
        Thread.sleep(100);

        java.util.Timer watcherTimer = new java.util.Timer();
        watcherTimer.schedule(watcherTimer(), 5000, 5000);

        writerFuture.get();
        readerFuture.get();

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
    }
}
