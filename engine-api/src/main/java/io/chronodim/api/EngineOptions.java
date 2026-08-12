package io.chronodim.api;

/**
 * Engine tuning knobs. {@link #defaults()} is production-safe; use
 * {@link #builder()} to override. All sizes in bytes, durations as named.
 */
public record EngineOptions(
        StorageBackend storageBackend,
        long walSegmentBytes,
        long groupCommitWindowMicros,
        int groupCommitMaxTxns,
        long memtableFlushBytes,
        long blockCacheBytes,
        int maxSegmentsBeforeCompaction,
        int maxManifestErrors,
        boolean fsync,
        boolean publishEnabled,
        long publishIntervalMillis,
        String objectStoreUri,
        long walShipIntervalMillis,
        boolean numericLoadIds) {

    /**
     * ROCKSDB is the default hot store. LSM is the built-in pure-Java backend
     * (zero native dependencies) honoring the same StorageEngine contract (G7).
     */
    public enum StorageBackend { ROCKSDB, LSM }

    public static EngineOptions defaults() {
        return builder().build();
    }

    public EngineOptions {
        if (storageBackend == null) storageBackend = StorageBackend.ROCKSDB;
        if (walSegmentBytes < 1 << 16) throw new ConfigException("walSegmentBytes must be >= 64KiB");
        if (groupCommitWindowMicros < 0) throw new ConfigException("groupCommitWindowMicros must be >= 0");
        if (groupCommitMaxTxns < 1) throw new ConfigException("groupCommitMaxTxns must be >= 1");
        if (memtableFlushBytes < 1 << 16) throw new ConfigException("memtableFlushBytes must be >= 64KiB");
        if (maxSegmentsBeforeCompaction < 2) throw new ConfigException("maxSegmentsBeforeCompaction must be >= 2");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.storageBackend = storageBackend;
        b.walSegmentBytes = walSegmentBytes;
        b.groupCommitWindowMicros = groupCommitWindowMicros;
        b.groupCommitMaxTxns = groupCommitMaxTxns;
        b.memtableFlushBytes = memtableFlushBytes;
        b.blockCacheBytes = blockCacheBytes;
        b.maxSegmentsBeforeCompaction = maxSegmentsBeforeCompaction;
        b.maxManifestErrors = maxManifestErrors;
        b.fsync = fsync;
        b.publishEnabled = publishEnabled;
        b.publishIntervalMillis = publishIntervalMillis;
        b.objectStoreUri = objectStoreUri;
        b.walShipIntervalMillis = walShipIntervalMillis;
        b.numericLoadIds = numericLoadIds;
        return b;
    }

    public static final class Builder {
        private StorageBackend storageBackend = StorageBackend.ROCKSDB;
        private long walSegmentBytes = 64L << 20;        // 64MB segments (R-WAL-3)
        private long groupCommitWindowMicros = 2_000;    // 2ms group commit window (R-WAL-1)
        private int groupCommitMaxTxns = 512;            // or 512 txns, whichever first
        private long memtableFlushBytes = 64L << 20;
        private long blockCacheBytes = 512L << 20;
        private int maxSegmentsBeforeCompaction = 8;
        private int maxManifestErrors = 100;
        private boolean fsync = true;
        private boolean publishEnabled = true;
        private long publishIntervalMillis = 1_000;
        private String objectStoreUri;
        private long walShipIntervalMillis = 5_000;
        private boolean numericLoadIds = false;

        public Builder storageBackend(StorageBackend v) { storageBackend = v; return this; }
        public Builder walSegmentBytes(long v) { walSegmentBytes = v; return this; }
        public Builder groupCommitWindowMicros(long v) { groupCommitWindowMicros = v; return this; }
        public Builder groupCommitMaxTxns(int v) { groupCommitMaxTxns = v; return this; }
        public Builder memtableFlushBytes(long v) { memtableFlushBytes = v; return this; }
        public Builder blockCacheBytes(long v) { blockCacheBytes = v; return this; }
        public Builder maxSegmentsBeforeCompaction(int v) { maxSegmentsBeforeCompaction = v; return this; }
        public Builder maxManifestErrors(int v) { maxManifestErrors = v; return this; }
        /** fsync=false is for tests/benchmarks only; it forfeits durability guarantees. */
        public Builder fsync(boolean v) { fsync = v; return this; }
        public Builder publishEnabled(boolean v) { publishEnabled = v; return this; }
        public Builder publishIntervalMillis(long v) { publishIntervalMillis = v; return this; }
        public Builder objectStoreUri(String v) { objectStoreUri = v; return this; }
        public Builder walShipIntervalMillis(long v) { walShipIntervalMillis = v; return this; }
        /** Enforce numeric (BIGINT-range) load ids: digits only, 0 < id <= Long.MAX_VALUE. */
        public Builder numericLoadIds(boolean v) { numericLoadIds = v; return this; }

        public EngineOptions build() {
            return new EngineOptions(storageBackend, walSegmentBytes, groupCommitWindowMicros, groupCommitMaxTxns,
                    memtableFlushBytes, blockCacheBytes, maxSegmentsBeforeCompaction, maxManifestErrors, fsync,
                    publishEnabled, publishIntervalMillis, objectStoreUri, walShipIntervalMillis, numericLoadIds);
        }
    }
}
