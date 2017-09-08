package com.upserve.uppend;

import com.upserve.uppend.lookup.LongLookup;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

import com.upserve.uppend.benchmark.*;


@Slf4j
public class Main {
    public static final int EXIT_CODE_USAGE = 2;
    public static final int EXIT_CODE_UNEXPECTED_FAIL = 4;

    public static final String READWRITE = "ReadWrite";
    public static final String READ = "Read";
    public static final String WRITE = "Write";

    public Writer writer;
    public Reader reader;

    private final Random random = new Random();

    long range;
    int count;
    int maxPartitions;
    int maxKeys;
    int hashSize;
    int cacheSize;
    int sleep = 0;

    private final AppendOnlyStore testInstance;

    public Main(String mode, String location, int maxPartitions, int maxKeys, int count, int hashSize, int cachesize) {

        this.count = count;
        this.maxPartitions = maxPartitions; // max ~ 2000
        this.maxKeys = maxKeys; // max ~ 10,000,000
        this.hashSize = hashSize;
        this.cacheSize = cachesize;

        Path path = Paths.get(location);
        if (path.toFile().exists()) log.warn("Location already exists: appending to {}", path);

        testInstance = new FileAppendOnlyStoreBuilder()
                .withDir(path)
                .withLongLookupHashSize(hashSize)
                .withLongLookupWriteCacheSize(cachesize)
                .build();


        range = (long) maxPartitions * (long) maxKeys * 199;


        switch (mode) {

            case READWRITE:
                writer = simpleWriter();
                reader = simpleReader();
                sleep = 31;
                break;

            case READ:
                writer = Writer.noop();
                reader = simpleReader();
                break;

            case WRITE:
                writer = simpleWriter();
                reader = Reader.noop();
                break;
            default:
                throw new RuntimeException("Unknown mode: " + mode);
        }

    }

    private Writer simpleWriter(){
        return new Writer(
                random.longs(count, 0, range).parallel(),
                longInt -> {
                    byte[] myBytes = bytes(longInt);
                    testInstance.append(partition(longInt, maxPartitions), key(longInt/maxPartitions, maxKeys), myBytes);
                    return myBytes.length;
                }
        );
    }

    private Reader simpleReader() {
        return new Reader(
                random.longs(count, 0, range).parallel(),
                longInt -> testInstance.read(partition(longInt, maxPartitions), key(longInt/maxPartitions, maxKeys))
                            .mapToInt(theseBytes -> theseBytes.length)
                            .sum()
        );
    }

    public static String key(long integer, int maxKeys) {
        return String.format("%08X", integer % maxKeys);
    }

    public static String partition(long integer, int maxPartitions) {
        return String.format("_%04X", integer % maxPartitions);
    }

    public static byte[] bytes(long integer) {
        int length =(int) (integer % 65536);
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 0);
        return bytes;
    }

    public static void usage() {
        System.err.println("usage: java uppend.jar mode path/for/uppend/data partitions, keys, count");
    }

    public int run() throws InterruptedException {
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

        log.info("Main is All Done!");

        return 0;
    }

    public static void main(String... args) throws Exception {
        int status;
        try {
            int nargs = args.length;
            if (nargs < 2) {
                usage();
                status = EXIT_CODE_USAGE;
            } else {
                String mode = args[0];
                String path = args[1];

                int maxPartitions = nargs > 2 ? Integer.parseInt(args[2]) : 64;
                int maxKeys = nargs > 3 ? Integer.parseInt(args[3]) : 1000;
                int count = nargs > 4 ? Integer.parseInt(args[4]) : 100_000;
                int hashSize = nargs > 5 ? Integer.parseInt(args[5]) : LongLookup.DEFAULT_HASH_SIZE;
                int cacheSize = nargs > 6 ? Integer.parseInt(args[6]) : LongLookup.DEFAULT_WRITE_CACHE_SIZE;


                Main main = new Main(mode, path, maxPartitions, maxKeys, count, hashSize, cacheSize);

                status = main.run();
            }
        } catch (Throwable t) {
            log.error("Unexpected Failure", t);
            status = EXIT_CODE_UNEXPECTED_FAIL;
        }

        System.exit(status);
    }
}

