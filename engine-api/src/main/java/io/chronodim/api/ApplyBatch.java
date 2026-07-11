package io.chronodim.api;

import java.util.List;

/**
 * A single engine transaction: one or more table batches applied atomically
 * (R-APPLY-4) under one caller-supplied idempotency key (R-APPLY-3).
 */
public record ApplyBatch(String loadId, List<TableBatch> tables) {

    public record TableBatch(String table, List<InputRow> rows) {
        public TableBatch {
            if (table == null || table.isBlank()) throw new ValidationException("table batch: table name required");
            rows = List.copyOf(rows);
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
    }

    public static ApplyBatch single(String loadId, String table, List<InputRow> rows) {
        return new ApplyBatch(loadId, List.of(new TableBatch(table, rows)));
    }
}
