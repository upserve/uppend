package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.blobs.NativeIO;
import com.upserve.uppend.metrics.*;

import java.nio.file.Path;

public class FileStoreBuilder<T extends FileStoreBuilder<T>> {

    // Long lookup Cache Options
    public static final int DEFAULT_PARTITION_COUNT = 0;
    public static final int DEFAULT_LOOKUP_HASH_COUNT = 256;
    public static final int DEFAULT_LOOKUP_PAGE_SIZE =  NativeIO.pageSize * 64;

    public static final int TARGET_PRODUCTION_BUFFER_SIZE = Integer.MAX_VALUE;

    public static final int DEFAULT_METADATA_PAGE_SIZE = NativeIO.pageSize;
    public static final int DEFAULT_METADATA_TTL = 0; // Off by default!

    private String storeName = "";
    private int partitionCount = DEFAULT_PARTITION_COUNT;
    private int lookupHashCount = DEFAULT_LOOKUP_HASH_COUNT;

    private int lookupPageSize = DEFAULT_LOOKUP_PAGE_SIZE;

    private int metadataTTL = DEFAULT_METADATA_TTL;
    private int metadataPageSize = DEFAULT_METADATA_PAGE_SIZE;

    private int targetBufferSize = TARGET_PRODUCTION_BUFFER_SIZE;

    private String writeLockContentString = null;

    // Store Options
    public static final int DEFAULT_FLUSH_DELAY_SECONDS = 30;
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000;
    private int flushDelaySeconds = DEFAULT_FLUSH_DELAY_SECONDS;
    private int flushThreshold = DEFAULT_FLUSH_THRESHOLD;
    private Path dir = null;
    private MetricRegistry storeMetricsRegistry = null;
    private String metricsRootName = "";
    private String metricsInstanceID = null;
    private boolean storeMetrics = false;
    private MetricRegistry cacheMetricsRegistry = null;
    private boolean cacheMetrics = false;

    private final LookupDataMetrics.Adders lookupDataMetricsAdders = new LookupDataMetrics.Adders();
    private final MutableBlobStoreMetrics.Adders mutableBlobStoreMetricsAdders = new MutableBlobStoreMetrics.Adders();
    private final LongBlobStoreMetrics.Adders longBlobStoreMetricsAdders = new LongBlobStoreMetrics.Adders();


    // Long lookup Cache Options
    @SuppressWarnings("unchecked")
    public T withLongLookupHashCount(int longLookupHashCount) {
        this.lookupHashCount = longLookupHashCount;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withLookupPageSize(int lookupPageSize) {
        if (lookupPageSize % NativeIO.pageSize != 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Illegal lookupPageSize %d; Must be a multiple of the host system page size: %d",
                            lookupPageSize, NativeIO.pageSize
                    )
            );
        }
        this.lookupPageSize = lookupPageSize;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withMetadataPageSize(int metadataPageSize) {
        if (metadataPageSize % NativeIO.pageSize != 0){
            throw new IllegalArgumentException(
                    String.format(
                            "Illegal metadataPageSize %d; Must be a multiple of the host system page size: %d",
                            metadataPageSize, NativeIO.pageSize
                    )
            );
        }
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

    @SuppressWarnings("unchecked")
    public T withWriteLockContentString(String writeLockContentString) {
        this.writeLockContentString = writeLockContentString;
        return (T) this;
    }

    // Append Store Options
    @SuppressWarnings("unchecked")
    public T withStoreName(String storeName) {
        this.storeName = storeName;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
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
     * Used to differentiate multiple open instances of the same table
     * @param metricsInstanceID the name under which to register metrics from this instance of the store
     * @return the builder
     */
    @SuppressWarnings("unchecked")
    public T withMetricsInstanceID(String metricsInstanceID) {
        this.metricsInstanceID = metricsInstanceID;
        return (T) this;
    }

    public int getLookupHashCount() {
        return lookupHashCount;
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

    public int getPartitionCount(){ return partitionCount; }

    public String getMetricsRootName(){ return metricsRootName; }

    public String getMetricsInstanceID(){ return metricsInstanceID; }

    public String getWriteLockContentString() { return writeLockContentString; }

    public LookupDataMetrics.Adders getLookupDataMetricsAdders(){ return lookupDataMetricsAdders; }

    public MutableBlobStoreMetrics.Adders getMutableBlobStoreMetricsAdders() { return mutableBlobStoreMetricsAdders; }

    public LongBlobStoreMetrics.Adders getLongBlobStoreMetricsAdders() { return longBlobStoreMetricsAdders; }

    @Override
    public String toString() {
        return "FileStoreBuilder{" +
                "storeName='" + storeName + '\'' +
                ", partitionCount=" + partitionCount +
                ", lookupHashCount=" + lookupHashCount +
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
                ", writeLockContentString='" + writeLockContentString + "'" +
                '}';
    }
}
