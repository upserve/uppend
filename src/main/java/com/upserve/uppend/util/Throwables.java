package com.upserve.uppend.util;

public class Throwables {
    /**
     * Get the root cause of a throwable
     *
     * @param t throwable value
     * @return root cause of the given throwable value
     */
    public static Throwable getRootCause(Throwable t) {
        Throwable cause = t.getCause();
        return cause == null ? t : getRootCause(cause);
    }
}
