package com.upserve.uppend.util;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.*;

public class ConsumerBlockingStream<T> extends FilterStream<T> implements Consumer<T>, Stream<T>, Iterator<T> {
    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    private final AtomicBoolean isDoneConsuming;
    private final ArrayBlockingQueue<T> queue;

    public ConsumerBlockingStream() {
        this(DEFAULT_QUEUE_CAPACITY);
    }

    public ConsumerBlockingStream(int queueCapacity) {
        isDoneConsuming = new AtomicBoolean();
        queue = new ArrayBlockingQueue<T>(DEFAULT_QUEUE_CAPACITY);
        delegate = StreamSupport.stream(Spliterators.spliteratorUnknownSize())
    }

    private

    @Override
    public void accept(T t) {
        try {
            queue.put(t);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void completeAccepting() throws Exception {
        isDoneConsuming.set(true);
    }

    @Override
    public boolean hasNext() {
        while (!isDoneConsuming.get() && queue.isEmpty()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return !queue.isEmpty();
    }

    @Override
    public T next() {
        return queue.remove();
    }
}
