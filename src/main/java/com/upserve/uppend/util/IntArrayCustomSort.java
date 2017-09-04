package com.upserve.uppend.util;

import java.util.Arrays;

public class IntArrayCustomSort {
    /**
     * Functional interface for comparing primitive integers, similar to {@link java.util.Comparator#compare(Object, Object)}.
     */
    public interface IntComparatorFunction {
        /**
         * Compares two integers, similar to {@link java.util.Comparator#compare(Object, Object)}.
         *
         * @param a first integer to compare
         * @param b second integer to compare
         * @return a negative integer, zero, or a positive integer as the first
         *         argument is less than, equal to, or greater than the second
         */
        int compare(int a, int b);
    }

    /**
     * Sort a primitive integer array with a custom comparator.
     *
     * @param values the int[] to sort
     * @param comparatorFunction the {@link IntComparatorFunction} to compare ints
     */
    public static void sort(int[] values, IntComparatorFunction comparatorFunction) {
        // TODO: implement primitive parallelSort with comparator so we don't have to copy
        int len = values.length;
        Integer[] vObj = new Integer[len];
        for (int i = 0; i < len; i++) {
            vObj[i] = values[i];
        }
        Arrays.parallelSort(vObj, comparatorFunction::compare);
        for (int i = 0; i < len; i++) {
            values[i] = vObj[i];
        }
    }
}
