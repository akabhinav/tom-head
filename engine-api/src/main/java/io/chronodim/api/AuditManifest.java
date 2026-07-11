package io.chronodim.api;

import java.util.List;

/**
 * Immutable, durable record of one apply/load (R-APPLY-7). Written as part of the
 * commit itself; retrievable forever by load id or txn id.
 */
public record AuditManifest(
        String loadId,
        long txnId,
        long startedAtMicros,
        long wallClockMillis,
        String walSegment,
        long walStartOffset,
        long walEndOffset,
        boolean alreadyApplied,
        boolean backfill,
        List<TableStats> tables,
        List<RowError> errors) {

    /** Per-table outcome counters. */
    public record TableStats(
            String table,
            String configHash,
            int schemaVersion,
            long rowsIn,
            long inserts,
            long updates,
            long noOps,
            long deletes,
            long lateSplits,
            long rejects,
            long quarantined) {}

    /** One rejected/failed row (capped in count; full detail goes to quarantine when configured). */
    public record RowError(String table, long rowIndex, String reason) {}

    public AuditManifest {
        tables = List.copyOf(tables);
        errors = List.copyOf(errors);
    }
}
