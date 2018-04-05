package com.upserve.uppend.blobs;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public class PageKey {
    private final Path filePath;
    private final int page;

    PageKey(Path filePath, int page){
        this.filePath = filePath;
        this.page = page;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageKey pageKey = (PageKey) o;
        return page == pageKey.page &&
                Objects.equals(filePath, pageKey.filePath);
    }

    @Override
    public int hashCode() {

        return Objects.hash(filePath, page);
    }

    Path getFilePath() {
        return filePath;
    }

    int getPage() {
        return page;
    }
}
