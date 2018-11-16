package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.stats.*;
import com.upserve.uppend.metrics.MetricsStatsCounter;

import java.nio.file.Path;
import java.util.function.Supplier;

public class FileStoreBuilder<T extends FileStoreBuilder<T>> {

    // Long lookup Cache Options
    public static final int DEFAULT_PARTITION_SIZE = 0;
    public static final int DEFAULT_LOOKUP_HASH_SIZE = 256;
    public static final int DEFAULT_LOOKUP_PAGE_SIZE = 256 * 1024;

    public static final int TARGET_PRODUCTION_BUFFER_SIZE = Integer.MAX_VALUE;

    public static final int DEFAULT_METADATA_PAGE_SIZE = 4096;
    public static final int DEFAULT_METADATA_TTL = 0; // Off by default!

    private String storeName = "";
    private int partitionSize = DEFAULT_PARTITION_SIZE;
    private int lookupHashSize = DEFAULT_LOOKUP_HASH_SIZE;

    private int lookupPageSize = DEFAULT_LOOKUP_PAGE_SIZE;

    private int metadataTTL = DEFAULT_METADATA_TTL;
    private int metadataPageSize = DEFAULT_METADATA_PAGE_SIZE;

    private int targetBufferSize = TARGET_PRODUCTION_BUFFER_SIZE;

    // Store Options
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000;
    private int flushDelaySeconds = DEFAULT_FLUSH_DELAY_SECONDS;
    private int flushThreshold = DEFAULT_FLUSH_THRESHOLD;
    private Path dir = null;
    private MetricRegistry storeMetricsRegistry = null;
    private String metricsRootName = "";
    private boolean storeMetrics = false;
    private MetricRegistry cacheMetricsRegistry = null;
    private boolean cacheMetrics = false;

    // Long lookup Cache Options
    @SuppressWarnings("unchecked")
    public T withLongLookupHashSize(int longLookupHashSize) {
        this.lookupHashSize = longLookupHashSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupPageSize(int lookupPageSize) {
        this.lookupPageSize = lookupPageSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMetadataPageSize(int metadataPageSize) {
        this.metadataPageSize = metadataPageSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMetadataTTL(int metadataTTL) {
        this.metadataTTL = metadataTTL;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withTargetBufferSize(int targetBufferSize) {
        this.targetBufferSize = targetBufferSize;
        return (T) this;
    }

    // Append Store Options
    @SuppressWarnings("unchecked")
    public T withStoreName(String storeName) {
        this.storeName = storeName;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartitionSize(int partitionSize) {
        this.partitionSize = partitionSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withFlushDelaySeconds(int flushDelaySeconds) {
        this.flushDelaySeconds = flushDelaySeconds;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withFlushThreshold(int flushThreshold) {
        this.flushThreshold = flushThreshold;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withDir(Path dir) {
        this.dir = dir;
        return (T) this;
    }

    /**
     * The builder will wrap the built store in a class that computes storeMetrics for each operation
     *
     * @param metrics a CodaHale storeMetrics registry
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withStoreMetrics(MetricRegistry metrics) {
        this.storeMetricsRegistry = metrics;
        this.storeMetrics = true;
        return (T) this;
    }

    /**
     * Use a root name for all metrics
     * @param metricsRootName the root name under which to register metrics from this store
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withMetricsRootName(String metricsRootName) {
        this.metricsRootName = metricsRootName;
        return (T) this;
    }

    /**
     * Return a StatsCounterSupplier for use in the Caffeine builder or null if cacheMetrics is false
     *
     * @param elements the string elements to use in registering metrics for this cache
     * @return the Supplier or null
     */
    public Supplier<StatsCounter> metricsSupplier(String... elements) {
        if (!cacheMetrics) return null;
        if (cacheMetricsRegistry != null) {
            return () -> new MetricsStatsCounter(cacheMetricsRegistry, String.join(".", elements));
        } else {
            return ConcurrentStatsCounter::new;
        }
    }
    public int getLookupHashSize() {
        return lookupHashSize;
    }

    public int getLookupPageSize() {
        return lookupPageSize;
    }

    public boolean isStoreMetrics() {
        return storeMetrics;
    }

    public int getFlushDelaySeconds() {
        return flushDelaySeconds;
    }

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public Path getDir() {
        return dir;
    }

    public MetricRegistry getStoreMetricsRegistry() {
        return storeMetricsRegistry;
    }

    public int getMetadataPageSize() {
        return metadataPageSize;
    }

    public int getMetadataTTL() {
        return metadataTTL;
    }

    public int getTargetBufferSize() {
        return targetBufferSize;
    }

    public String getStoreName() {
        return storeName.isEmpty() ? getDir().getFileName().toString() : storeName;
    }

    public int getPartitionSize(){ return partitionSize; }

    public String getMetricsRootName(){ return metricsRootName; }

    @Override
    public String toString() {
        return "FileStoreBuilder{" +
                "storeName='" + storeName + '\'' +
                ", partitionSize=" + partitionSize +
                ", lookupHashSize=" + lookupHashSize +
                ", lookupPageSize=" + lookupPageSize +
                ", metadataTTL=" + metadataTTL +
                ", metadataPageSize=" + metadataPageSize +
                ", targetBufferSize=" + targetBufferSize +
                ", flushDelaySeconds=" + flushDelaySeconds +
                ", flushThreshold=" + flushThreshold +
                ", dir=" + dir +
                ", storeMetricsRegistry=" + storeMetricsRegistry +
                ", metricsRootName='" + metricsRootName + '\'' +
                ", storeMetrics=" + storeMetrics +
                ", cacheMetricsRegistry=" + cacheMetricsRegistry +
                ", cacheMetrics=" + cacheMetrics +
                '}';
    }
}
