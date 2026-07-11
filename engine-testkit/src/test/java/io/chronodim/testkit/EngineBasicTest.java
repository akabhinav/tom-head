package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.LockException;
import io.chronodim.api.Op;
import io.chronodim.api.QuarantineEntry;
import io.chronodim.api.TableConfig;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableConfigIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineBasicTest {

    @TempDir
    Path dir;

    static EngineOptions fastOptions() {
        return EngineOptions.builder().groupCommitWindowMicros(0).build();
    }

    private static final String CUSTOMER_YAML = """
            table: customer
            business_key: [customer_id]
            schema:
              - {name: customer_id, type: string}
              - {name: name,        type: string}
              - {name: risk_score,  type: double}
              - {name: updated_at,  type: timestamp}
            tracked_columns: [name, risk_score]
            ignored_columns: [updated_at]
            valid_time:
              mode: source_column
              column: updated_at
            quality_gates:
              - {column: customer_id, rule: not_null}
              - {column: risk_score,  rule: "between 0 and 1000"}
            """;

    private static InputRow row(String id, String name, double risk, String ts) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("customer_id", id);
        v.put("name", name);
        v.put("risk_score", risk);
        v.put("updated_at", ts);
        return InputRow.upsert(v);
    }

    private static long ts(String iso) {
        return ColumnType.parseTimestampMicros(iso);
    }

    @Test
    void endToEndLifecycle() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            assertEquals(List.of("customer"), e.listTables());

            // Insert
            AuditManifest m1 = e.apply(ApplyBatch.single("load-1", "customer",
                    List.of(row("C1", "Alice", 100, "2024-01-01T00:00:00Z"))));
            assertEquals(1, m1.tables().get(0).inserts());

            Version cur = e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow();
            assertEquals("Alice", cur.row().get("name"));
            assertTrue(cur.current());
            assertTrue(cur.validToIsOpen());

            // Update closes previous version at the new valid_from
            e.apply(ApplyBatch.single("load-2", "customer",
                    List.of(row("C1", "Alice Smith", 120, "2024-02-01T00:00:00Z"))));
            List<Version> hist = e.getHistory("customer", Map.of("customer_id", "C1"));
            assertEquals(3, hist.size()); // new open version, closed rewrite, original belief
            assertEquals("Alice Smith", hist.get(0).row().get("name"));
            assertEquals(ts("2024-02-01T00:00:00Z"), hist.get(0).validFrom());

            // As-of reads: inclusive start, exclusive end
            assertEquals("Alice", e.getAsOf("customer", Map.of("customer_id", "C1"),
                    ts("2024-01-31T23:59:59Z")).orElseThrow().row().get("name"));
            assertEquals("Alice Smith", e.getAsOf("customer", Map.of("customer_id", "C1"),
                    ts("2024-02-01T00:00:00Z")).orElseThrow().row().get("name"));
            assertTrue(e.getAsOf("customer", Map.of("customer_id", "C1"), ts("2023-01-01T00:00:00Z")).isEmpty());

            // No-op: same tracked values, different ignored column
            AuditManifest m3 = e.apply(ApplyBatch.single("load-3", "customer",
                    List.of(row("C1", "Alice Smith", 120, "2024-03-15T08:00:00Z"))));
            assertEquals(1, m3.tables().get(0).noOps());
            assertEquals(3, e.getHistory("customer", Map.of("customer_id", "C1")).size());

            // Idempotency (R-APPLY-3)
            AuditManifest again = e.apply(ApplyBatch.single("load-2", "customer",
                    List.of(row("C1", "SHOULD NOT APPLY", 999, "2024-06-01T00:00:00Z"))));
            assertTrue(again.alreadyApplied());
            assertEquals("Alice Smith", e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow().row().get("name"));

            // Soft delete
            Map<String, Object> del = new LinkedHashMap<>();
            del.put("customer_id", "C1");
            del.put("updated_at", "2024-04-01T00:00:00Z");
            e.apply(ApplyBatch.single("load-4", "customer", List.of(InputRow.delete(del))));
            assertTrue(e.getCurrent("customer", Map.of("customer_id", "C1")).isEmpty());
            assertEquals("Alice Smith", e.getAsOf("customer", Map.of("customer_id", "C1"),
                    ts("2024-03-01T00:00:00Z")).orElseThrow().row().get("name"));
            assertEquals(Op.DELETE, e.getHistory("customer", Map.of("customer_id", "C1")).get(0).op());

            // Re-insert after delete
            e.apply(ApplyBatch.single("load-5", "customer",
                    List.of(row("C1", "Alice Reborn", 50, "2024-05-01T00:00:00Z"))));
            Version reborn = e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow();
            assertEquals("Alice Reborn", reborn.row().get("name"));
            assertEquals(Op.INSERT, reborn.op());
        }
    }

    @Test
    void bitemporalCorrection() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            e.apply(ApplyBatch.single("b1", "customer", List.of(row("C1", "V-later", 10, "2024-03-01T00:00:00Z"))));
            long tx1 = e.getHistory("customer", Map.of("customer_id", "C1")).get(0).txTime();

            // Late arrival lands BEFORE the existing version → split (R-APPLY-5)
            AuditManifest m = e.apply(ApplyBatch.single("b2", "customer",
                    List.of(row("C1", "V-earlier", 5, "2024-01-01T00:00:00Z"))));
            assertEquals(1, m.tables().get(0).lateSplits());

            // Latest knowledge: earlier version closed at the later one's start
            Version early = e.getAsOf("customer", Map.of("customer_id", "C1"), ts("2024-02-01T00:00:00Z")).orElseThrow();
            assertEquals("V-earlier", early.row().get("name"));
            assertEquals(ts("2024-03-01T00:00:00Z"), early.validTo());

            // Bitemporal (R-READ-5): before the correction the engine knew nothing about January
            assertTrue(e.getAsOf("customer", Map.of("customer_id", "C1"), ts("2024-02-01T00:00:00Z"), tx1).isEmpty());
            // ...but it did know the March version
            assertEquals("V-later", e.getAsOf("customer", Map.of("customer_id", "C1"),
                    ts("2024-04-01T00:00:00Z"), tx1).orElseThrow().row().get("name"));
        }
    }

    @Test
    void crossTableTransactionAndFailBatch() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            e.createTable(TableConfigIO.fromYaml("""
                    table: adjustment
                    business_key: [adj_id]
                    on_row_error: fail_batch
                    schema:
                      - {name: adj_id, type: string}
                      - {name: amount, type: "decimal(18,2)"}
                    quality_gates:
                      - {column: amount, rule: not_null}
                    """));

            // Atomic cross-table commit (R-APPLY-4)
            AuditManifest m = e.apply(new ApplyBatch("x1", List.of(
                    new ApplyBatch.TableBatch("customer", List.of(row("C9", "Nine", 9, "2024-01-01T00:00:00Z"))),
                    new ApplyBatch.TableBatch("adjustment", List.of(InputRow.upsert(Map.of("adj_id", "A1", "amount", "10.50")))))));
            assertEquals(2, m.tables().size());
            assertEquals(m.txnId(), e.getCurrent("adjustment", Map.of("adj_id", "A1")).orElseThrow().txnId());

            // fail_batch on table 2 must abort table 1's changes too
            assertThrows(ValidationException.class, () -> e.apply(new ApplyBatch("x2", List.of(
                    new ApplyBatch.TableBatch("customer", List.of(row("C10", "Ten", 10, "2024-01-01T00:00:00Z"))),
                    new ApplyBatch.TableBatch("adjustment", List.of(InputRow.upsert(new LinkedHashMap<>(Map.of("adj_id", "A2")))))))));
            assertTrue(e.getCurrent("customer", Map.of("customer_id", "C10")).isEmpty(), "aborted batch must leave no trace");
            assertTrue(e.manifest("x2").isEmpty());
        }
    }

    @Test
    void quarantineFlow() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml("""
                    table: q
                    business_key: [id]
                    on_row_error: quarantine
                    schema:
                      - {name: id,   type: string}
                      - {name: score, type: long}
                    quality_gates:
                      - {column: score, rule: "between 0 and 100"}
                    """));
            AuditManifest m = e.apply(ApplyBatch.single("q1", "q", List.of(
                    InputRow.upsert(Map.of("id", "ok", "score", 50)),
                    InputRow.upsert(Map.of("id", "bad", "score", 5000)))));
            assertEquals(1, m.tables().get(0).inserts());
            assertEquals(1, m.tables().get(0).quarantined());

            List<QuarantineEntry> q = e.quarantineList("q", 10);
            assertEquals(1, q.size());
            assertEquals("bad", q.get(0).values().get("id"));

            // Widen the gate, re-apply the quarantined row, entry disappears.
            e.alterTable(TableConfigIO.fromYaml("""
                    table: q
                    business_key: [id]
                    schema:
                      - {name: id,   type: string}
                      - {name: score, type: long}
                    """));
            AuditManifest rm = e.quarantineReapply("q", List.of(), "q1-retry");
            assertEquals(1, rm.tables().get(0).inserts());
            assertTrue(e.quarantineList("q", 10).isEmpty());
            assertEquals(5000L, e.getCurrent("q", Map.of("id", "bad")).orElseThrow().row().get("score"));
        }
    }

    @Test
    void schemaEvolutionAndAlterRules() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            e.apply(ApplyBatch.single("s1", "customer", List.of(row("C1", "A", 1, "2024-01-01T00:00:00Z"))));

            // Add a nullable column (R-CFG-4)
            e.alterTable(TableConfigIO.fromYaml(CUSTOMER_YAML.replace(
                    "  - {name: updated_at,  type: timestamp}",
                    "  - {name: updated_at,  type: timestamp}\n  - {name: country,     type: string}")));
            assertEquals(2, e.describeTable("customer").currentSchema().schemaVersion());

            // Old versions up-convert with null for the new column
            Version v = e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow();
            assertEquals(1, v.schemaVersion());
            assertTrue(v.row().containsKey("country"));
            assertEquals(null, v.row().get("country"));

            // New writes carry the new schema version
            Map<String, Object> nv = new LinkedHashMap<>();
            nv.put("customer_id", "C1");
            nv.put("name", "B");
            nv.put("risk_score", 2.0);
            nv.put("updated_at", "2024-02-01T00:00:00Z");
            nv.put("country", "CH");
            e.apply(ApplyBatch.single("s2", "customer", List.of(InputRow.upsert(nv))));
            Version v2 = e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow();
            assertEquals(2, v2.schemaVersion());
            assertEquals("CH", v2.row().get("country"));

            // Incompatible changes rejected (R-CFG-3)
            assertThrows(ConfigException.class, () -> e.alterTable(TableConfigIO.fromYaml(
                    CUSTOMER_YAML.replace("risk_score,  type: double", "risk_score,  type: string"))));
            assertThrows(ConfigException.class, () -> e.alterTable(TableConfigIO.fromYaml(
                    CUSTOMER_YAML.replace("business_key: [customer_id]", "business_key: [name]"))));
        }
    }

    @Test
    void duplicateValidFromPolicies() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            // last_wins (default): the second row for the same instant wins
            e.apply(ApplyBatch.single("d1", "customer", List.of(
                    row("C1", "First", 1, "2024-01-01T00:00:00Z"),
                    row("C1", "Second", 2, "2024-01-01T00:00:00Z"))));
            assertEquals("Second", e.getCurrent("customer", Map.of("customer_id", "C1")).orElseThrow().row().get("name"));
            assertEquals(1, e.getHistory("customer", Map.of("customer_id", "C1")).size());
        }
    }

    @Test
    void scansStreamConsistentState() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            List<InputRow> rows = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                rows.add(row("K" + i, "N" + i, i, "2024-01-01T00:00:00Z"));
            }
            e.apply(ApplyBatch.single("scan1", "customer", rows));
            Map<String, Object> delKey = new LinkedHashMap<>();
            delKey.put("customer_id", "K7");
            delKey.put("updated_at", "2024-02-01T00:00:00Z");
            e.apply(ApplyBatch.single("scan2", "customer", List.of(InputRow.delete(delKey))));

            int n = 0;
            try (VersionCursor c = e.scanCurrent("customer")) {
                while (c.hasNext()) {
                    Version v = c.next();
                    assertTrue(v.current());
                    assertNotEquals("K7", v.row().get("customer_id"));
                    n++;
                }
            }
            assertEquals(499, n);

            int asOf = 0;
            try (VersionCursor c = e.scanAsOf("customer", ts("2024-01-15T00:00:00Z"))) {
                while (c.hasNext()) {
                    c.next();
                    asOf++;
                }
            }
            assertEquals(500, asOf, "K7 was still alive in January");
        }
    }

    @Test
    void manifestsAndVerifyAndLock() {
        try (Engine e = ChronoDim.open(dir, fastOptions())) {
            e.createTable(TableConfigIO.fromYaml(CUSTOMER_YAML));
            e.apply(ApplyBatch.single("m1", "customer", List.of(row("C1", "A", 1, "2024-01-01T00:00:00Z"))));

            Optional<AuditManifest> m = e.manifest("m1");
            assertTrue(m.isPresent());
            assertTrue(m.get().walSegment().startsWith("wal-"));
            assertEquals(1, e.listManifests("customer", 10).size());

            Map<String, Object> verify = e.verify();
            assertTrue(verify.containsKey("state_fingerprint"));

            // Second writer must be rejected (NG2 / R-DUR-4)
            assertThrows(LockException.class, () -> ChronoDim.open(dir, fastOptions()));
        }
    }
}
