package io.chronodim.storage;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Internal hot-KV abstraction (R-STORE-1). Everything above this interface is
 * storage-implementation agnostic: v1 ships a pure-Java LSM
 * ({@link io.chronodim.storage.lsm.LsmStorageEngine}); a RocksDB adapter can be
 * dropped in without changing any public API or on-disk contract above this layer.
 *
 * <p>Contract notes:
 * <ul>
 *   <li>Keys sort by unsigned lexicographic byte order.</li>
 *   <li>{@link #write} is atomic and durable-to-memtable; the engine's own WAL is the
 *       durability authority (the storage engine keeps no WAL of its own).</li>
 *   <li>{@link #durableTxn()} is the highest engine txn id fully contained in
 *       crash-safe storage (segments referenced from the storage manifest). On
 *       restart, the engine replays its WAL strictly after this watermark.</li>
 * </ul>
 */
public interface StorageEngine extends AutoCloseable {

    /** Point lookup; null when absent. */
    byte[] get(byte[] key);

    /** Batched point lookups (R-PERF-3). Result list is positionally aligned; null when absent. */
    List<byte[]> multiGet(List<byte[]> keys);

    /** Snapshot-consistent iterator over all keys with the given prefix, ascending. */
    CloseableKvIterator prefixScan(byte[] prefix);

    /** Applies all mutations atomically, tagged with the creating engine txn. */
    void write(AtomicBatch batch, long txnId);

    /** Consistent point-in-time read view. Cheap (O(1) rotation); always close. */
    KvSnapshot snapshot();

    /**
     * Bulk path (R-APPLY-8): ingest an iterator of strictly ascending unique keys
     * directly as an on-disk segment, bypassing the memtable. {@code expectedKeys}
     * sizes the bloom filter (an estimate is fine; it affects only the FP rate).
     */
    void ingestSorted(Iterator<KV> sortedKvs, long txnId, long expectedKeys);

    /** Flushes all in-memory state and writes a self-contained checkpoint into {@code dir}. */
    void checkpoint(Path dir);

    /** Forces all memtables to disk and persists the manifest. */
    void flush();

    /** Highest engine txn id covered by crash-safe on-disk state. */
    long durableTxn();

    /** Implementation statistics for observability. */
    Map<String, Object> stats();

    @Override
    void close();
}
