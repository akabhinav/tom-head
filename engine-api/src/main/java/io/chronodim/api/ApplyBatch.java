package io.chronodim.api;

import java.util.List;
import java.util.Map;

/**
 * A single engine transaction: one or more table batches applied atomically
 * (R-APPLY-4) under one caller-supplied idempotency key (R-APPLY-3).
 * {@code metadata} is free-form audit context recorded in the manifest forever.
 */
public record ApplyBatch(String loadId, List<TableBatch> tables, Map<String, Object> metadata) {

    /**
     * Rows for one table. {@code fullSnapshot=true} declares the rows to be the
     * complete current population: any active entity of the table whose business
     * key is absent from the rows is soft-deleted in the same transaction
     * (delete-by-absence for feeds that never send operations).
     */
    public record TableBatch(String table, List<InputRow> rows, boolean fullSnapshot) {
        public TableBatch {
            if (table == null || table.isBlank()) throw new ValidationException("table batch: table name required");
            rows = List.copyOf(rows);
        }

        public TableBatch(String table, List<InputRow> rows) {
            this(table, rows, false);
        }
    }

    public ApplyBatch {
        if (loadId == null || loadId.isBlank()) throw new ValidationException("apply: load_id is required (idempotency key)");
        if (loadId.length() > 512) throw new ValidationException("apply: load_id longer than 512 chars");
        if (tables == null || tables.isEmpty()) throw new ValidationException("apply: at least one table batch required");
        tables = List.copyOf(tables);
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (TableBatch t : tables) {
            if (!seen.add(t.table())) {
                throw new ValidationException("apply: table '" + t.table() + "' appears twice in one batch — merge its rows");
            }
        }
        metadata = metadata == null ? Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(metadata));
    }

    public ApplyBatch(String loadId, List<TableBatch> tables) {
        this(loadId, tables, Map.of());
    }

    public static ApplyBatch single(String loadId, String table, List<InputRow> rows) {
        return new ApplyBatch(loadId, List.of(new TableBatch(table, rows)), Map.of());
    }
}
