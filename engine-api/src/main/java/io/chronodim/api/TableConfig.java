package io.chronodim.api;

import java.util.List;

/**
 * Declarative table definition (R-CFG-1). The full schema history is retained so
 * stored versions written under older schema versions can be up-converted on read.
 */
public record TableConfig(
        String table,
        List<String> businessKey,
        List<TableSchema> schemaHistory,
        List<String> trackedColumns,
        List<String> ignoredColumns,
        ValidTimeMode validTimeMode,
        String validTimeColumn,
        LateArrivalPolicy lateArrivalPolicy,
        DuplicatePolicy duplicatePolicy,
        DeleteMode deleteMode,
        BatchFailurePolicy batchFailurePolicy,
        List<QualityGate> qualityGates,
        PublishConfig publish) {

    public enum ValidTimeMode { SOURCE_COLUMN, LOAD_TIME }
    public enum LateArrivalPolicy { SPLIT, REJECT, QUARANTINE }
    /** Same key + same valid_from within one batch (R-APPLY-5). */
    public enum DuplicatePolicy { LAST_WINS, REJECT }
    public enum DeleteMode { SOFT, IGNORE }
    /** What to do with rows failing coercion/quality gates (R-APPLY-6). */
    public enum BatchFailurePolicy { FAIL_BATCH, SKIP_ROWS, QUARANTINE }

    public record PublishConfig(boolean enabled, String location, List<String> partitionBy) {
        public PublishConfig {
            partitionBy = partitionBy == null ? List.of() : List.copyOf(partitionBy);
            if (enabled && (location == null || location.isBlank())) {
                throw new ConfigException("publish.enabled requires publish.location");
            }
        }
        public static PublishConfig disabled() { return new PublishConfig(false, null, List.of()); }
    }

    public TableConfig {
        if (table == null || !table.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new ConfigException("table name '" + table + "' must match [A-Za-z_][A-Za-z0-9_]*");
        }
        if (businessKey == null || businessKey.isEmpty()) throw new ConfigException("table '" + table + "': business_key required");
        businessKey = List.copyOf(businessKey);
        if (schemaHistory == null || schemaHistory.isEmpty()) throw new ConfigException("table '" + table + "': schema required");
        schemaHistory = List.copyOf(schemaHistory);
        for (int i = 0; i < schemaHistory.size(); i++) {
            if (schemaHistory.get(i).schemaVersion() != i + 1) {
                throw new ConfigException("table '" + table + "': schema history must be contiguous starting at version 1");
            }
        }
        trackedColumns = trackedColumns == null ? List.of() : List.copyOf(trackedColumns);
        ignoredColumns = ignoredColumns == null ? List.of() : List.copyOf(ignoredColumns);
        qualityGates = qualityGates == null ? List.of() : List.copyOf(qualityGates);
        if (validTimeMode == null) validTimeMode = ValidTimeMode.LOAD_TIME;
        if (lateArrivalPolicy == null) lateArrivalPolicy = LateArrivalPolicy.SPLIT;
        if (duplicatePolicy == null) duplicatePolicy = DuplicatePolicy.LAST_WINS;
        if (deleteMode == null) deleteMode = DeleteMode.SOFT;
        if (batchFailurePolicy == null) batchFailurePolicy = BatchFailurePolicy.SKIP_ROWS;
        if (publish == null) publish = PublishConfig.disabled();

        TableSchema current = schemaHistory.getLast();
        for (String k : businessKey) {
            Column bc = current.column(k);
            if (bc == null) throw new ConfigException("table '" + table + "': business_key column '" + k + "' not in schema");
            if (!bc.isScalar()) {
                throw new ConfigException("table '" + table + "': business_key column '" + k + "' must be a scalar type, not " + bc.typeDeclaration());
            }
        }
        for (String c : trackedColumns) {
            if (current.column(c) == null) throw new ConfigException("table '" + table + "': tracked column '" + c + "' not in schema");
            if (businessKey.contains(c)) throw new ConfigException("table '" + table + "': business key column '" + c + "' cannot be tracked");
        }
        for (String c : ignoredColumns) {
            if (current.column(c) == null) throw new ConfigException("table '" + table + "': ignored column '" + c + "' not in schema");
            if (trackedColumns.contains(c)) throw new ConfigException("table '" + table + "': column '" + c + "' both tracked and ignored");
        }
        if (validTimeMode == ValidTimeMode.SOURCE_COLUMN) {
            if (validTimeColumn == null) throw new ConfigException("table '" + table + "': valid_time.mode=source_column requires valid_time.column");
            Column vc = current.column(validTimeColumn);
            if (vc == null) throw new ConfigException("table '" + table + "': valid_time.column '" + validTimeColumn + "' not in schema");
            ColumnType vk = vc.kind();
            if (vk != ColumnType.TIMESTAMP && vk != ColumnType.TIMESTAMP_NTZ && vk != ColumnType.DATE) {
                throw new ConfigException("table '" + table + "': valid_time.column must be timestamp, timestamp_ntz or date");
            }
        }
        for (String pc : publish.partitionBy()) {
            Column col = current.column(pc);
            if (col == null) {
                throw new ConfigException("table '" + table + "': publish.partition_by column '" + pc + "' not in schema");
            }
            if (!col.isScalar()) {
                throw new ConfigException("table '" + table + "': publish.partition_by column '" + pc
                        + "' must be a scalar type, not " + col.typeDeclaration());
            }
        }
        for (QualityGate g : qualityGates) {
            if (current.column(g.column()) == null) {
                throw new ConfigException("table '" + table + "': quality gate references unknown column '" + g.column() + "'");
            }
        }
    }

    /** The latest schema version. */
    public TableSchema currentSchema() {
        return schemaHistory.getLast();
    }

    public TableSchema schemaAt(int version) {
        if (version < 1 || version > schemaHistory.size()) {
            throw new CorruptionException("table '" + table + "': unknown schema_version " + version);
        }
        return schemaHistory.get(version - 1);
    }

    /**
     * Columns participating in the change-detection hash: the configured
     * {@code tracked_columns}, or — when none are configured — every non-key,
     * non-ignored column of the current schema.
     */
    public List<String> effectiveTrackedColumns() {
        if (!trackedColumns.isEmpty()) return trackedColumns;
        return currentSchema().columns().stream()
                .map(Column::name)
                .filter(n -> !businessKey.contains(n) && !ignoredColumns.contains(n))
                .toList();
    }
}
