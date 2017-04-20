package com.upserve.uppend.util;

public class Partition {
    public static void validate(String partition) {
        if (!verifier(partition)) {
            throw new IllegalArgumentException("Partition must be non-empty, consist of valid Java identifier characters and /, and have zero-width parts surrounded by /: " + partition);
        }
    }

    protected static boolean verifier(String text) {
        return verifier(text, 0);
    }

    private static boolean verifier(String text, int startIndex) {
        int slash = text.indexOf('/', startIndex);
        int endIndexExclusive;

        // If there is a slash, check the content after it
        if (slash != -1) {
            if (!verifier(text, slash + 1)) {
                return false;
            }
            endIndexExclusive = slash;

            // If there is no slash, continue with the whole string
        } else {
            endIndexExclusive = text.length();
        }

        // Fail empty strings
        if (startIndex == endIndexExclusive) {
            return false;
        }

        // Fail strings that don't start with a proper starting char
        if (!isValidPartitionStart(text.charAt(startIndex))) {
            return false;
        }

        // Allow single char strings that pass the check above
        if (endIndexExclusive == startIndex + 1) {
            return true;
        }

        // Check each character in the string
        for (int i = startIndex; i < endIndexExclusive; i++) {
            if (!isValidPartitionPart(text.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    private static boolean isValidPartitionStart(char c) {
        return Character.isJavaIdentifierStart(c) || Character.isDigit(c);
    }

    private static boolean isValidPartitionPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }
}
