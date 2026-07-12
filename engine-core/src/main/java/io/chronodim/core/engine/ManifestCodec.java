package io.chronodim.core.engine;

import io.chronodim.api.AuditManifest;
import io.chronodim.core.util.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AuditManifest ↔ JSON (frozen schema, R-APPLY-7 / §11.6). */
public final class ManifestCodec {
    private ManifestCodec() {}

    public static String toJson(AuditManifest m) {
        Map<String, Object> j = new LinkedHashMap<>();
        j.put("format_version", 1L);
        j.put("load_id", m.loadId());
        j.put("txn_id", m.txnId());
        j.put("started_at_micros", m.startedAtMicros());
        j.put("wall_clock_millis", m.wallClockMillis());
        j.put("wal_segment", m.walSegment());
        j.put("wal_start_offset", m.walStartOffset());
        j.put("wal_end_offset", m.walEndOffset());
        j.put("backfill", m.backfill());
        List<Object> tables = new ArrayList<>();
        for (AuditManifest.TableStats t : m.tables()) {
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("table", t.table());
            tm.put("config_hash", t.configHash());
            tm.put("schema_version", (long) t.schemaVersion());
            tm.put("rows_in", t.rowsIn());
            tm.put("inserts", t.inserts());
            tm.put("updates", t.updates());
            tm.put("no_ops", t.noOps());
            tm.put("deletes", t.deletes());
            tm.put("late_splits", t.lateSplits());
            tm.put("rejects", t.rejects());
            tm.put("quarantined", t.quarantined());
            tables.add(tm);
        }
        j.put("tables", tables);
        List<Object> errors = new ArrayList<>();
        for (AuditManifest.RowError e : m.errors()) {
            Map<String, Object> em = new LinkedHashMap<>();
            em.put("table", e.table());
            em.put("row_index", e.rowIndex());
            em.put("reason", e.reason());
            errors.add(em);
        }
        j.put("errors", errors);
        if (!m.metadata().isEmpty()) j.put("metadata", m.metadata());
        return Json.write(j);
    }

    @SuppressWarnings("unchecked")
    public static AuditManifest fromJson(String json, boolean alreadyApplied) {
        Map<String, Object> j = Json.parseObject(json);
        List<AuditManifest.TableStats> tables = new ArrayList<>();
        for (Object o : (List<Object>) j.getOrDefault("tables", List.of())) {
            Map<String, Object> t = (Map<String, Object>) o;
            tables.add(new AuditManifest.TableStats(
                    (String) t.get("table"),
                    (String) t.get("config_hash"),
                    ((Number) t.get("schema_version")).intValue(),
                    num(t, "rows_in"), num(t, "inserts"), num(t, "updates"), num(t, "no_ops"),
                    num(t, "deletes"), num(t, "late_splits"), num(t, "rejects"), num(t, "quarantined")));
        }
        List<AuditManifest.RowError> errors = new ArrayList<>();
        for (Object o : (List<Object>) j.getOrDefault("errors", List.of())) {
            Map<String, Object> e = (Map<String, Object>) o;
            errors.add(new AuditManifest.RowError((String) e.get("table"), num(e, "row_index"), (String) e.get("reason")));
        }
        return new AuditManifest(
                (String) j.get("load_id"),
                num(j, "txn_id"),
                num(j, "started_at_micros"),
                num(j, "wall_clock_millis"),
                (String) j.get("wal_segment"),
                num(j, "wal_start_offset"),
                num(j, "wal_end_offset"),
                alreadyApplied,
                Boolean.TRUE.equals(j.get("backfill")),
                tables,
                errors,
                (Map<String, Object>) j.getOrDefault("metadata", Map.of()));
    }

    private static long num(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? 0 : ((Number) v).longValue();
    }
}
