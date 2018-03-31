package com.upserve.uppend.blobs;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public class PageKey {
    private final Path fileName;
    private final FileChannel chan;
    private final int page;

    PageKey(Path fileName, FileChannel chan, int page){
        this.fileName = fileName;
        this.chan = chan;
        this.page = page;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageKey pageKey = (PageKey) o;
        return page == pageKey.page &&
                Objects.equals(chan, pageKey.chan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chan, page);
    }

    Path getFileName() {
        return fileName;
    }

    FileChannel getChan() {
        return chan;
    }

    int getPage() {
        return page;
    }
}
