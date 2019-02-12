package com.upserve.uppend.util;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class LongStreams {
    public static LongStream lazyConcat(LongStream a, Supplier<LongStream> b) {
        PrimitiveIterator.OfLong lazyIter = new LazyConcatLongIter(a, b);
        return StreamSupport.longStream(Spliterators.spliteratorUnknownSize(lazyIter, Spliterator.ORDERED), false);
    }

    private static class LazyConcatLongIter implements PrimitiveIterator.OfLong {
        private PrimitiveIterator.OfLong iter;
        private Supplier<LongStream> next;

        LazyConcatLongIter(LongStream a, Supplier<LongStream> b) {
            this.iter = a.iterator();
            this.next = b;
        }

        @Override
        public boolean hasNext() {
            if (iter.hasNext()) {
                return true;
            }

            if (next == null) {
                return false;
            }

            swap();
            return iter.hasNext();
        }

        @Override
        public long nextLong() {
            try {
                return iter.nextLong();
            } catch (NoSuchElementException e) {
                if (next == null) {
                    throw e;
                }
                swap();
                return iter.nextLong();
            }
        }

        private void swap() {
            iter = next.get().iterator();
            next = null;
        }

    }
}
