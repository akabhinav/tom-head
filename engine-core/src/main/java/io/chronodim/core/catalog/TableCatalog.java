package io.chronodim.core.catalog;

import io.chronodim.api.Column;
import io.chronodim.api.ConfigException;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.core.codec.Codecs;
import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.StorageEngine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory table registry backed by the META keyspace. Mutations (create/alter)
 * are prepared here but committed through the engine's WAL pipeline (CONFIG_CHANGE),
 * so catalog changes are as durable and replayable as data (R-CFG-3/4).
 */
public final class TableCatalog {

    public record TableRuntime(TableConfig config, int tableId, String configHash) {
        public List<Column> businessKeyColumns() {
            TableSchema s = config.currentSchema();
            List<Column> out = new ArrayList<>(config.businessKey().size());
            for (String k : config.businessKey()) out.add(s.column(k));
            return out;
        }
    }

    private static final byte SUB_TABLE = 1;

    private final ConcurrentHashMap<String, TableRuntime> tables = new ConcurrentHashMap<>();
    private volatile int maxTableId;

    /** Loads all table definitions from storage (at open, and after WAL replay). */
    public void load(StorageEngine storage) {
        tables.clear();
        maxTableId = 0;
        byte[] prefix = {Codecs.KS_META, SUB_TABLE};
        try (CloseableKvIterator it = storage.prefixScan(prefix)) {
            while (it.hasNext()) {
                KV kv = it.next();
                TableConfigIO.Stored stored = TableConfigIO.fromStoredJson(new String(kv.value(), StandardCharsets.UTF_8));
                register(stored.config(), stored.tableId());
            }
        }
    }

    public void register(TableConfig config, int tableId) {
        tables.put(config.table(), new TableRuntime(config, tableId, TableConfigIO.configHash(config)));
        if (tableId > maxTableId) maxTableId = tableId;
    }

    public TableRuntime get(String table) {
        TableRuntime rt = tables.get(table);
        if (rt == null) throw new ConfigException("unknown table '" + table + "'");
        return rt;
    }

    public TableRuntime getIfExists(String table) {
        return tables.get(table);
    }

    public List<String> names() {
        return tables.keySet().stream().sorted().toList();
    }

    public List<TableRuntime> all() {
        return tables.values().stream().sorted(java.util.Comparator.comparing(r -> r.config().table())).toList();
    }

    /** Prepares the KV mutation for a create; the engine commits it. */
    public TableRuntime prepareCreate(TableConfig config, AtomicBatch batch) {
        if (tables.containsKey(config.table())) {
            throw new ConfigException("table '" + config.table() + "' already exists");
        }
        int id = maxTableId + 1;
        batch.put(Codecs.metaKey(SUB_TABLE, config.table()),
                TableConfigIO.toStoredJson(config, id).getBytes(StandardCharsets.UTF_8));
        return new TableRuntime(config, id, TableConfigIO.configHash(config));
    }

    /**
     * Validates an alter against the existing table and prepares the mutation.
     * Allowed: adding nullable columns (bumps schema_version), changing tracked/
     * ignored columns, policies, quality gates, publish settings. Rejected:
     * business key changes, column type changes or removals (R-CFG-3).
     */
    public TableRuntime prepareAlter(TableConfig incoming, AtomicBatch batch) {
        TableRuntime existing = get(incoming.table());
        TableConfig old = existing.config();

        if (!old.businessKey().equals(incoming.businessKey())) {
            throw new ConfigException("table '" + incoming.table() + "': business_key cannot change ("
                    + old.businessKey() + " -> " + incoming.businessKey() + ")");
        }

        TableSchema oldSchema = old.currentSchema();
        TableSchema newDeclared = incoming.currentSchema();
        for (Column oc : oldSchema.columns()) {
            Column nc = newDeclared.column(oc.name());
            if (nc == null) {
                throw new ConfigException("table '" + incoming.table() + "': column '" + oc.name() + "' cannot be removed");
            }
            if (!nc.type().equals(oc.type())) {
                throw new ConfigException("table '" + incoming.table() + "': column '" + oc.name()
                        + "' type change " + oc.typeDeclaration() + " -> " + nc.typeDeclaration() + " is not allowed");
            }
        }
        // Columns must be append-only so stored payload positions stay valid per version.
        for (int i = 0; i < oldSchema.columns().size(); i++) {
            if (!newDeclared.columns().get(i).name().equals(oldSchema.columns().get(i).name())) {
                throw new ConfigException("table '" + incoming.table() + "': existing columns cannot be reordered");
            }
        }

        List<TableSchema> history = new ArrayList<>(old.schemaHistory());
        boolean schemaChanged = newDeclared.columns().size() != oldSchema.columns().size();
        if (schemaChanged) {
            history.add(new TableSchema(oldSchema.schemaVersion() + 1, newDeclared.columns()));
        }

        TableConfig merged = new TableConfig(
                incoming.table(), old.businessKey(), history,
                incoming.trackedColumns(), incoming.ignoredColumns(),
                incoming.validTimeMode(), incoming.validTimeColumn(),
                incoming.lateArrivalPolicy(), incoming.duplicatePolicy(), incoming.deleteMode(),
                incoming.batchFailurePolicy(), incoming.qualityGates(), incoming.publish(),
                incoming.absentColumns());

        batch.put(Codecs.metaKey(SUB_TABLE, merged.table()),
                TableConfigIO.toStoredJson(merged, existing.tableId()).getBytes(StandardCharsets.UTF_8));
        return new TableRuntime(merged, existing.tableId(), TableConfigIO.configHash(merged));
    }

    public Map<String, Object> describe(String table) {
        TableRuntime rt = get(table);
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("table", rt.config().table());
        m.put("table_id", (long) rt.tableId());
        m.put("config_hash", rt.configHash());
        m.put("schema_version", (long) rt.config().currentSchema().schemaVersion());
        m.put("stored_config", io.chronodim.core.util.Json.parseObject(TableConfigIO.toStoredJson(rt.config(), rt.tableId())));
        return m;
    }
}
