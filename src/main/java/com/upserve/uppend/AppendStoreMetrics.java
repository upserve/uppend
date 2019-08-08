package com.upserve.uppend;

import com.upserve.uppend.metrics.*;

public interface AppendStoreMetrics {
    BlockedLongMetrics getBlockedLongMetrics();
    BlobStoreMetrics getBlobStoreMetrics();
}
