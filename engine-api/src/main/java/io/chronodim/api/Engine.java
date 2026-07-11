package io.chronodim.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The ChronoDim engine (R-API-1). Thread-safe: any number of concurrent readers;
 * {@code apply}/{@code backfill} calls are serialized internally through the
 * single-writer commit pipeline.
 */
public interface Engine extends AutoCloseable {

    // ---- catalog -----------------------------------------------------------

    /** Creates a table from a validated config. Fails if the table exists. */
    void createTable(TableConfig config);

    /**
     * Applies a config change (R-CFG-3/4). Only compatible evolutions are accepted:
     * adding nullable columns (bumps schema_version), changing quality gates,
     * policies, or publish settings. Business key and column type changes are rejected.
     */
    void alterTable(TableConfig config);

    List<String> listTables();

    TableConfig describeTable(String table);

    // ---- writes ------------------------------------------------------------

    /**
     * Applies a batch of changes as one atomic, durable, idempotent transaction.
     * Re-applying a committed {@code loadId} is a no-op returning the original
     * manifest with {@code alreadyApplied=true}.
     */
    AuditManifest apply(ApplyBatch batch);

    /**
     * Bulk backfill (R-APPLY-8): initial load through the sorted-ingest path.
     * WAL-fenced and manifest-producing, but not row-by-row logged. The table
     * must be empty.
     */
    AuditManifest backfill(String loadId, String table, Iterable<InputRow> rows);

    // ---- reads (R-READ) ----------------------------------------------------

    /** Latest non-deleted version of the entity, if any. */
    Optional<Version> getCurrent(String table, Map<String, Object> businessKey);

    /** Version whose [valid_from, valid_to) contains validTimeMicros, by latest knowledge. */
    Optional<Version> getAsOf(String table, Map<String, Object> businessKey, long validTimeMicros);

    /** Bitemporal read (R-READ-5): what the engine believed at txTimeMicros about validTimeMicros. */
    Optional<Version> getAsOf(String table, Map<String, Object> businessKey, long validTimeMicros, long txTimeMicros);

    /** All versions of the entity, newest first (by valid_from, then tx_time). Includes superseded versions. */
    List<Version> getHistory(String table, Map<String, Object> businessKey);

    /** Streaming scan of all current (non-deleted, latest-knowledge) versions. */
    VersionCursor scanCurrent(String table);

    /** Streaming scan of all versions valid at validTimeMicros by latest knowledge. */
    VersionCursor scanAsOf(String table, long validTimeMicros);

    // ---- manifests / quarantine ---------------------------------------------

    Optional<AuditManifest> manifest(String loadId);

    List<AuditManifest> listManifests(String table, int limit);

    List<QuarantineEntry> quarantineList(String table, int limit);

    /**
     * Re-applies quarantined rows (by id, or all for the table when ids is empty)
     * under a new load id, removing successfully applied entries.
     */
    AuditManifest quarantineReapply(String table, List<Long> ids, String loadId);

    // ---- operations ----------------------------------------------------------

    /** Order-independent state fingerprint + per-table counts (R-DUR-5). */
    Map<String, Object> verify();

    /** Flushes and checkpoints storage; returns snapshot metadata (txn id, path). */
    Map<String, Object> snapshot();

    /** Engine statistics for observability (R-OBS). */
    Map<String, Object> stats();

    /** Highest committed transaction id. */
    long lastCommittedTxn();

    @Override
    void close();
}
