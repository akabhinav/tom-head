package io.chronodim.core.catalog;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.QualityGate;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.core.util.Json;
import io.chronodim.core.util.Yaml;
import io.chronodim.storage.util.XxHash64;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * TableConfig ↔ YAML (user-facing, R-CFG-1) and ↔ canonical JSON (stored form,
 * carrying the full schema history). The config hash (R-CFG-5) is xxHash64 of the
 * canonical JSON minus the volatile {@code table_id}.
 */
public final class TableConfigIO {
    private TableConfigIO() {}

    // ---- user YAML → config -----------------------------------------------------

    public static TableConfig fromYaml(String yaml) {
        Map<String, Object> m = Yaml.parseMap(yaml);
        return fromMap(m);
    }

    @SuppressWarnings("unchecked")
    public static TableConfig fromMap(Map<String, Object> m) {
        String table = str(m.get("table"), "table");
        List<String> businessKey = strList(m.get("business_key"), "business_key");

        Object schemaObj = m.get("schema");
        if (!(schemaObj instanceof List<?> schemaList) || schemaList.isEmpty()) {
            throw new ConfigException("table '" + table + "': schema must be a non-empty list");
        }
        List<Column> columns = new ArrayList<>();
        for (Object o : schemaList) {
            if (!(o instanceof Map<?, ?> cm)) throw new ConfigException("schema entries must be {name, type} mappings");
            columns.add(ColumnType.parseDeclaration(str(cm.get("name"), "schema.name"), str(cm.get("type"), "schema.type")));
        }
        int schemaVersion = m.get("schema_version") == null ? 1 : ((Number) m.get("schema_version")).intValue();
        List<TableSchema> history = new ArrayList<>();
        for (int v = 1; v < schemaVersion; v++) {
            // Older schema versions are only known from the stored form; a fresh YAML
            // declares the current schema. History is reconstructed by the catalog on alter.
            throw new ConfigException("table '" + table + "': schema_version in YAML must be 1 (history is engine-managed)");
        }
        history.add(new TableSchema(1, columns));

        Map<String, Object> validTime = subMap(m.get("valid_time"));
        TableConfig.ValidTimeMode vtMode = TableConfig.ValidTimeMode.LOAD_TIME;
        String vtColumn = null;
        if (validTime != null) {
            String mode = str(validTime.getOrDefault("mode", "load_time"), "valid_time.mode");
            vtMode = switch (mode.toLowerCase(Locale.ROOT)) {
                case "source_column" -> TableConfig.ValidTimeMode.SOURCE_COLUMN;
                case "load_time" -> TableConfig.ValidTimeMode.LOAD_TIME;
                default -> throw new ConfigException("valid_time.mode must be source_column|load_time");
            };
            vtColumn = validTime.get("column") == null ? null : String.valueOf(validTime.get("column"));
        }

        Map<String, Object> late = subMap(m.get("late_arrival"));
        TableConfig.LateArrivalPolicy latePolicy = TableConfig.LateArrivalPolicy.SPLIT;
        TableConfig.DuplicatePolicy dupPolicy = TableConfig.DuplicatePolicy.LAST_WINS;
        if (late != null) {
            if (late.get("policy") != null) {
                latePolicy = enumOf(TableConfig.LateArrivalPolicy.class, late.get("policy"), "late_arrival.policy");
            }
            if (late.get("same_batch_duplicates") != null) {
                dupPolicy = enumOf(TableConfig.DuplicatePolicy.class, late.get("same_batch_duplicates"), "late_arrival.same_batch_duplicates");
            }
        }

        Map<String, Object> deletes = subMap(m.get("deletes"));
        TableConfig.DeleteMode deleteMode = deletes == null || deletes.get("mode") == null
                ? TableConfig.DeleteMode.SOFT
                : enumOf(TableConfig.DeleteMode.class, deletes.get("mode"), "deletes.mode");

        TableConfig.BatchFailurePolicy failPolicy = TableConfig.BatchFailurePolicy.SKIP_ROWS;
        if (m.get("on_row_error") != null) {
            String v = String.valueOf(m.get("on_row_error")).toLowerCase(Locale.ROOT);
            failPolicy = switch (v) {
                case "fail_batch" -> TableConfig.BatchFailurePolicy.FAIL_BATCH;
                case "skip_rows", "skip" -> TableConfig.BatchFailurePolicy.SKIP_ROWS;
                case "quarantine" -> TableConfig.BatchFailurePolicy.QUARANTINE;
                default -> throw new ConfigException("on_row_error must be fail_batch|skip_rows|quarantine");
            };
        }

        List<QualityGate> gates = new ArrayList<>();
        Object qg = m.get("quality_gates");
        if (qg instanceof List<?> ql) {
            for (Object o : ql) {
                if (!(o instanceof Map<?, ?> gm)) throw new ConfigException("quality_gates entries must be {column, rule} mappings");
                gates.add(new QualityGate(str(gm.get("column"), "quality_gates.column"), str(gm.get("rule"), "quality_gates.rule")));
            }
        }

        Map<String, Object> pub = subMap(m.get("publish"));
        TableConfig.PublishConfig publish = TableConfig.PublishConfig.disabled();
        if (pub != null) {
            boolean enabled = Boolean.TRUE.equals(pub.get("enabled"));
            String location = pub.get("location") == null ? null : String.valueOf(pub.get("location"));
            List<String> partitionBy = pub.get("partition_by") == null ? List.of() : strList(pub.get("partition_by"), "publish.partition_by");
            TableConfig.PublishColumnStyle style = pub.get("column_style") == null
                    ? TableConfig.PublishColumnStyle.CHRONODIM
                    : enumOf(TableConfig.PublishColumnStyle.class, pub.get("column_style"), "publish.column_style");
            String startCol = null;
            String endCol = null;
            Boolean includeOps = null;
            Map<String, Object> scd2 = subMap(pub.get("scd2_columns"));
            if (scd2 != null) {
                startCol = scd2.get("start") == null ? null : String.valueOf(scd2.get("start"));
                endCol = scd2.get("end") == null ? null : String.valueOf(scd2.get("end"));
                if (scd2.get("include_ops") != null) includeOps = Boolean.valueOf(String.valueOf(scd2.get("include_ops")));
            }
            publish = new TableConfig.PublishConfig(enabled, location, partitionBy, style, startCol, endCol, includeOps);
        }

        return new TableConfig(table, businessKey, history,
                strListOrEmpty(m.get("tracked_columns"), "tracked_columns"),
                strListOrEmpty(m.get("ignored_columns"), "ignored_columns"),
                vtMode, vtColumn, latePolicy, dupPolicy, deleteMode, failPolicy, gates, publish);
    }

    // ---- config ↔ stored JSON ----------------------------------------------------

    public static String toStoredJson(TableConfig c, int tableId) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("format_version", 1L);
        m.put("table_id", (long) tableId);
        m.put("table", c.table());
        m.put("business_key", c.businessKey());
        List<Object> history = new ArrayList<>();
        for (TableSchema s : c.schemaHistory()) {
            Map<String, Object> sm = new LinkedHashMap<>();
            sm.put("schema_version", (long) s.schemaVersion());
            List<Object> cols = new ArrayList<>();
            for (Column col : s.columns()) {
                Map<String, Object> cm = new LinkedHashMap<>();
                cm.put("name", col.name());
                cm.put("type", col.typeDeclaration());
                cols.add(cm);
            }
            sm.put("columns", cols);
            history.add(sm);
        }
        m.put("schema_history", history);
        m.put("tracked_columns", c.trackedColumns());
        m.put("ignored_columns", c.ignoredColumns());
        m.put("valid_time_mode", c.validTimeMode().name());
        m.put("valid_time_column", c.validTimeColumn());
        m.put("late_arrival_policy", c.lateArrivalPolicy().name());
        m.put("duplicate_policy", c.duplicatePolicy().name());
        m.put("delete_mode", c.deleteMode().name());
        m.put("batch_failure_policy", c.batchFailurePolicy().name());
        List<Object> gates = new ArrayList<>();
        for (QualityGate g : c.qualityGates()) {
            Map<String, Object> gm = new LinkedHashMap<>();
            gm.put("column", g.column());
            gm.put("rule", g.rule());
            gates.add(gm);
        }
        m.put("quality_gates", gates);
        Map<String, Object> pub = new LinkedHashMap<>();
        pub.put("enabled", c.publish().enabled());
        pub.put("location", c.publish().location());
        pub.put("partition_by", c.publish().partitionBy());
        pub.put("column_style", c.publish().columnStyle().name());
        if (c.publish().startColumn() != null) {
            Map<String, Object> scd2 = new LinkedHashMap<>();
            scd2.put("start", c.publish().startColumn());
            scd2.put("end", c.publish().endColumn());
            if (c.publish().includeOps() != null) scd2.put("include_ops", c.publish().includeOps());
            pub.put("scd2_columns", scd2);
        } else if (c.publish().includeOps() != null) {
            pub.put("scd2_columns", Map.of("include_ops", c.publish().includeOps()));
        }
        m.put("publish", pub);
        return Json.write(m);
    }

    public record Stored(TableConfig config, int tableId) {}

    @SuppressWarnings("unchecked")
    public static Stored fromStoredJson(String json) {
        Map<String, Object> m = Json.parseObject(json);
        int tableId = ((Number) m.get("table_id")).intValue();
        String table = (String) m.get("table");
        List<String> businessKey = (List<String>) (List<?>) m.get("business_key");
        List<TableSchema> history = new ArrayList<>();
        for (Object o : (List<Object>) m.get("schema_history")) {
            Map<String, Object> sm = (Map<String, Object>) o;
            int ver = ((Number) sm.get("schema_version")).intValue();
            List<Column> cols = new ArrayList<>();
            for (Object co : (List<Object>) sm.get("columns")) {
                Map<String, Object> cm = (Map<String, Object>) co;
                cols.add(ColumnType.parseDeclaration((String) cm.get("name"), (String) cm.get("type")));
            }
            history.add(new TableSchema(ver, cols));
        }
        List<QualityGate> gates = new ArrayList<>();
        for (Object o : (List<Object>) m.getOrDefault("quality_gates", List.of())) {
            Map<String, Object> gm = (Map<String, Object>) o;
            gates.add(new QualityGate((String) gm.get("column"), (String) gm.get("rule")));
        }
        Map<String, Object> pub = (Map<String, Object>) m.get("publish");
        TableConfig.PublishConfig publish;
        if (pub == null) {
            publish = TableConfig.PublishConfig.disabled();
        } else {
            Map<String, Object> scd2 = (Map<String, Object>) pub.get("scd2_columns");
            publish = new TableConfig.PublishConfig(
                    Boolean.TRUE.equals(pub.get("enabled")),
                    (String) pub.get("location"),
                    pub.get("partition_by") == null ? List.of() : (List<String>) (List<?>) pub.get("partition_by"),
                    pub.get("column_style") == null ? TableConfig.PublishColumnStyle.CHRONODIM
                            : TableConfig.PublishColumnStyle.valueOf((String) pub.get("column_style")),
                    scd2 == null ? null : (String) scd2.get("start"),
                    scd2 == null ? null : (String) scd2.get("end"),
                    scd2 == null || scd2.get("include_ops") == null ? null : (Boolean) scd2.get("include_ops"));
        }
        TableConfig cfg = new TableConfig(table, businessKey, history,
                (List<String>) (List<?>) m.getOrDefault("tracked_columns", List.of()),
                (List<String>) (List<?>) m.getOrDefault("ignored_columns", List.of()),
                TableConfig.ValidTimeMode.valueOf((String) m.get("valid_time_mode")),
                (String) m.get("valid_time_column"),
                TableConfig.LateArrivalPolicy.valueOf((String) m.get("late_arrival_policy")),
                TableConfig.DuplicatePolicy.valueOf((String) m.get("duplicate_policy")),
                TableConfig.DeleteMode.valueOf((String) m.get("delete_mode")),
                TableConfig.BatchFailurePolicy.valueOf((String) m.get("batch_failure_policy")),
                gates, publish);
        return new Stored(cfg, tableId);
    }

    /** Config hash (R-CFG-5): canonical stored JSON with table_id fixed to 0. */
    public static String configHash(TableConfig c) {
        byte[] canon = toStoredJson(c, 0).getBytes(StandardCharsets.UTF_8);
        return String.format("%016x", XxHash64.hash(canon));
    }

    // ---- helpers ------------------------------------------------------------------

    private static String str(Object o, String field) {
        if (o == null) throw new ConfigException("missing required config field '" + field + "'");
        return String.valueOf(o);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> subMap(Object o) {
        if (o == null) return null;
        if (!(o instanceof Map)) throw new ConfigException("expected a mapping, got " + o.getClass().getSimpleName());
        return (Map<String, Object>) o;
    }

    private static List<String> strList(Object o, String field) {
        if (o == null) throw new ConfigException("missing required config field '" + field + "'");
        if (o instanceof String s) return List.of(s);
        if (o instanceof List<?> l) {
            List<String> out = new ArrayList<>();
            for (Object e : l) out.add(String.valueOf(e));
            return out;
        }
        throw new ConfigException("config field '" + field + "' must be a string or list of strings");
    }

    private static List<String> strListOrEmpty(Object o, String field) {
        return o == null ? List.of() : strList(o, field);
    }

    private static <E extends Enum<E>> E enumOf(Class<E> cls, Object v, String field) {
        try {
            return Enum.valueOf(cls, String.valueOf(v).trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConfigException("config field '" + field + "': invalid value '" + v + "'");
        }
    }
}
