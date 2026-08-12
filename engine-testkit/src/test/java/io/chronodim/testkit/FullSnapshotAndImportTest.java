package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ColumnType;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.Op;
import io.chronodim.api.TableConfig;
import io.chronodim.api.Version;
import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.scd2.Scd2Import;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Feeds with no operation column: delete-by-absence for full snapshots, and
 * import of rows that already carry SCD2 intervals (__START_AT/__END_AT).
 */
class FullSnapshotAndImportTest {

    @TempDir
    Path root;

    private static EngineOptions opts() {
        return EngineOptions.builder().groupCommitWindowMicros(0).publishEnabled(false).build();
    }

    private static long ts(String iso) {
        return ColumnType.parseTimestampMicros(iso);
    }

    @Test
    void fullSnapshotDeleteByAbsenceMatchesReference() {
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(41, new DataGen.StreamOptions(15, 0.0, 0.0, 0.0, 0.3));

        try (Engine e = ChronoDim.open(root.resolve("db"), opts())) {
            e.createTable(cfg);
            Random rnd = new Random(5);
            List<InputRow> lastSnapshot = null;
            for (int b = 0; b < 8; b++) {
                // A "snapshot" = random subset of keys, no _op anywhere.
                List<InputRow> all = gen.nextBatch(30);
                List<InputRow> snapshot = new ArrayList<>();
                for (InputRow r : all) {
                    if (rnd.nextDouble() < 0.8) snapshot.add(r);
                }
                AuditManifest m = e.apply(new ApplyBatch("snap-" + b,
                        List.of(new ApplyBatch.TableBatch("t", snapshot, true)), Map.of()));
                ref.applyBatch(snapshot, m.startedAtMicros(), true);
                DifferentialTest.compareFullState(e, ref, cfg);
                lastSnapshot = snapshot;
            }

            // Re-sending the identical snapshot is a pure no-op (nothing re-deleted).
            AuditManifest again = e.apply(new ApplyBatch("snap-final",
                    List.of(new ApplyBatch.TableBatch("t", lastSnapshot, true)), Map.of()));
            ref.applyBatch(lastSnapshot, again.startedAtMicros(), true);
            AuditManifest.TableStats st = again.tables().get(0);
            assertEquals(0, st.deletes(), "identical snapshot must delete nothing: " + st);
            assertEquals(0, st.inserts() + st.updates(), "identical snapshot must change nothing: " + st);
            DifferentialTest.compareFullState(e, ref, cfg);
        }
    }

    @Test
    void scd2IntervalImportReproducesChains() {
        TableConfig cfg = io.chronodim.core.catalog.TableConfigIO.fromYaml("""
                table: imp
                business_key: [id]
                schema:
                  - {name: id, type: string}
                  - {name: v,  type: string}
                """);

        try (Engine e = ChronoDim.open(root.resolve("db2"), opts())) {
            e.createTable(cfg);

            // A ready-made SCD2 export (Databricks shape): K1 has history + active row;
            // K2 ended (closed tail, no active row); K3 has a coverage gap (dead period).
            List<InputRow> raw = new ArrayList<>();
            raw.add(InputRow.upsert(Map.of("id", "K1", "v", "old", "__START_AT", "2026-01-01T00:00:00Z", "__END_AT", "2026-03-01T00:00:00Z")));
            raw.add(InputRow.upsert(Map.of("id", "K1", "v", "new", "__START_AT", "2026-03-01T00:00:00Z")));
            raw.add(InputRow.upsert(Map.of("id", "K2", "v", "gone", "__START_AT", "2026-01-01T00:00:00Z", "__END_AT", "2026-02-01T00:00:00Z")));
            raw.add(InputRow.upsert(Map.of("id", "K3", "v", "a", "__START_AT", "2026-01-01T00:00:00Z", "__END_AT", "2026-02-01T00:00:00Z")));
            raw.add(InputRow.upsert(Map.of("id", "K3", "v", "b", "__START_AT", "2026-05-01T00:00:00Z")));

            List<InputRow> shaped = Scd2Import.toInputRows(((EngineImpl) e).catalog().get("imp"), raw);
            e.apply(ApplyBatch.single("import-1", "imp", shaped));

            // K1: plain two-version chain, "new" active.
            Version k1 = e.getCurrent("imp", Map.of("id", "K1")).orElseThrow();
            assertEquals("new", k1.row().get("v"));
            assertEquals("old", e.getAsOf("imp", Map.of("id", "K1"), ts("2026-02-01T00:00:00Z")).orElseThrow().row().get("v"));
            assertEquals(ts("2026-03-01T00:00:00Z"),
                    e.getAsOf("imp", Map.of("id", "K1"), ts("2026-02-01T00:00:00Z")).orElseThrow().validTo());

            // K2: no active row — the closed tail became a soft delete at __END_AT.
            assertTrue(e.getCurrent("imp", Map.of("id", "K2")).isEmpty());
            assertEquals("gone", e.getAsOf("imp", Map.of("id", "K2"), ts("2026-01-15T00:00:00Z")).orElseThrow().row().get("v"));
            assertTrue(e.getAsOf("imp", Map.of("id", "K2"), ts("2026-02-15T00:00:00Z")).isEmpty());
            assertEquals(Op.DELETE, e.getHistory("imp", Map.of("id", "K2")).get(0).op());

            // K3: alive Jan, dead Feb-Apr (gap → tombstone), alive again from May.
            assertEquals("a", e.getAsOf("imp", Map.of("id", "K3"), ts("2026-01-15T00:00:00Z")).orElseThrow().row().get("v"));
            assertTrue(e.getAsOf("imp", Map.of("id", "K3"), ts("2026-03-15T00:00:00Z")).isEmpty(), "gap must read as dead");
            assertEquals("b", e.getCurrent("imp", Map.of("id", "K3")).orElseThrow().row().get("v"));
            assertEquals(Op.INSERT, e.getCurrent("imp", Map.of("id", "K3")).orElseThrow().op());

            // Idempotent: importing the same file again changes nothing.
            AuditManifest m2 = e.apply(ApplyBatch.single("import-2", "imp",
                    Scd2Import.toInputRows(((EngineImpl) e).catalog().get("imp"), raw)));
            AuditManifest.TableStats st = m2.tables().get(0);
            assertEquals(0, st.inserts() + st.updates() + st.deletes(), "re-import must be all no-ops: " + st);
        }
    }

    @Test
    void metadataTravelsIntoTheManifestForever() {
        TableConfig cfg = DataGen.testTable("t");
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("source", "finance-ops");
        meta.put("approved_by", "jsmith");
        meta.put("reason", "quarterly true-up");

        try (Engine e = ChronoDim.open(root.resolve("db3"), opts())) {
            e.createTable(cfg);
            DataGen gen = new DataGen(3, DataGen.StreamOptions.defaults());
            e.apply(new ApplyBatch("meta-1", List.of(new ApplyBatch.TableBatch("t", gen.nextBatch(5))), meta));
            // The engine adds reserved keys (e.g. _batch_hash); user metadata survives verbatim.
            Map<String, Object> stored = new java.util.LinkedHashMap<>(e.manifest("meta-1").orElseThrow().metadata());
            stored.keySet().removeIf(k -> k.startsWith("_"));
            assertEquals(meta, stored);
        }
        // Survives restart (it is part of the durable manifest).
        try (Engine e = ChronoDim.open(root.resolve("db3"), opts())) {
            assertEquals("jsmith", e.manifest("meta-1").orElseThrow().metadata().get("approved_by"));
        }
    }
}
