package com.upserve.uppend.util;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.*;

public class Futures {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void getAll(Collection<Future> futures) {
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                log.error("interrupted while flushing", e);
                Thread.interrupted();
            } catch (ExecutionException e) {
                log.error("exception executing flush", e);
            }
        });
    }
}
