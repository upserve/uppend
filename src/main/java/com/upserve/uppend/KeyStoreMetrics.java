package com.upserve.uppend;

import com.upserve.uppend.metrics.*;

public interface KeyStoreMetrics {

    LookupDataMetrics getLookupDataMetrics();
    LongBlobStoreMetrics getLongBlobStoreMetrics();
    MutableBlobStoreMetrics getMutableBlobStoreMetrics();

}
