package com.upserve.uppend;

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

    int range;
    int count;
    int maxPartitions;
    int maxKeys;
    int sleep = 0;

    private final AppendOnlyStore testInstance;

    public Main(String mode, String location, int maxPartitions, int maxKeys, int count) {

        this.count = count;
        this.maxPartitions = maxPartitions; // max ~ 2000
        this.maxKeys = maxKeys; // max ~ 10,000,000

        Path path = Paths.get(location);
        if (path.toFile().exists()) log.warn("Location already exists: appending to {}", path);

        testInstance = new FileAppendOnlyStoreBuilder().withDir(path).build();


        range = maxPartitions * maxKeys * 199;


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
                random.ints(count, 0, range).parallel(),
                integer -> {
                    byte[] myBytes = bytes(integer);
                    testInstance.append(partition(integer, maxPartitions), key(integer/maxPartitions, maxKeys), myBytes);
                    return myBytes.length;
                }
        );
    }

    private Reader simpleReader() {
        return new Reader(
                random.ints(count, 0, range).parallel(),
                integer -> testInstance.read(partition(integer, maxPartitions), key(integer/maxPartitions, maxKeys))
                            .mapToInt(theseBytes -> theseBytes.length)
                            .sum()
        );
    }

    public static String key(int integer, int maxKeys) {
        return String.format("%08X", integer % maxKeys);
    }

    public static String partition(int integer, int maxPartitions) {
        return String.format("_%04X", integer % maxPartitions);
    }

    public static byte[] bytes(int integer) {
        int length = (integer % 65536);
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
            while (true) {
                try {
                    long written = writer.bytesWritten.get();
                    long read = reader.bytesRead.get();
                    Thread.sleep(1000);
                    double writeRate = (writer.bytesWritten.get() - written) / (1024.0 * 1024.0);
                    double readRate = (reader.bytesRead.get() - read) / (1024.0 * 1024.0);
                    log.info(String.format("Read rate %5.2fmb/s, Write rate %5.2fmb/s", readRate, writeRate));
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


                Main main = new Main(mode, path, maxPartitions, maxKeys, count);

                status = main.run();
            }
        } catch (Throwable t) {
            log.error("Unexpected Failure", t);
            status = EXIT_CODE_UNEXPECTED_FAIL;
        }

        System.exit(status);
    }
}

