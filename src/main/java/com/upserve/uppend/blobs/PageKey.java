package com.upserve.uppend.blobs;

import java.nio.file.Path;
import java.util.Objects;

public class PageKey {
    private final Path filePath;
    private final long position;

    PageKey(Path filePath, long position){
        this.filePath = filePath;
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageKey pageKey = (PageKey) o;
        return position == pageKey.position &&
                Objects.equals(filePath, pageKey.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, position);
    }

    Path getFilePath() {
        return filePath;
    }

    long getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "PageKey{" +
                "filePath=" + filePath +
                ", position=" + position +
                '}';
    }
}
