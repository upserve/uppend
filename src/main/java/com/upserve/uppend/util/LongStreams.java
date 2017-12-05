package com.upserve.uppend.util;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.*;

public class LongStreams {
    public static LongStream lazyConcat(LongStream a, Supplier<LongStream> b) {
        PrimitiveIterator.OfLong lazyIter = new LazyConcatLongIter(a, b);
        return StreamSupport.longStream(Spliterators.spliteratorUnknownSize(lazyIter, Spliterator.ORDERED), false);
    }

    private static class LazyConcatLongIter implements PrimitiveIterator.OfLong {
        private PrimitiveIterator.OfLong iter;
        private Supplier<LongStream> next;

        private boolean havePeeked = false;
        private boolean peekHasNext;
        private long peekValue;

        LazyConcatLongIter(LongStream a, Supplier<LongStream> b) {
            this.iter = a.iterator();
            this.next = b;
        }

        private void peek() {
            if (!havePeeked) {
                if (peekHasNext = iter.hasNext()) {
                    peekValue = iter.next();
                } else if (next != null) {
                    iter = next.get().iterator();
                    next = null;
                    peek();
                }
                havePeeked = true;
            }
        }

        @Override
        public boolean hasNext() {
            peek();
            return peekHasNext;
        }

        @Override
        public long nextLong() {
            peek();
            havePeeked = false;
            return peekValue;
        }
    }
}
