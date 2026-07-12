package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ConfigException;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "primary key" of ChronoDim is the business_key — one column or a
 * combination. Uniqueness (one live entity, one open version) holds on the
 * FULL combination: (A1, AAPL) and (A1, MSFT) are different entities;
 * re-sending (A1, AAPL) versions that same entity. This test pins that down,
 * plus the constraint edges: NULL key parts rejected, key immutable via
 * alter, key columns must exist and be scalar.
 */
class CompositeKeyTest {

    @TempDir
    Path root;

    private static final String POSITION_YAML = """
            table: position
            business_key: [account_id, symbol]
            schema:
              - {name: account_id, type: string}
              - {name: symbol,     type: string}
              - {name: qty,        type: long}
              - {name: desk,      type: string}
            """;

    private static InputRow row(String acct, String sym, Long qty, String desk) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("account_id", acct);
        v.put("symbol", sym);
        v.put("qty", qty);
        v.put("desk", desk);
        return InputRow.upsert(v);
    }

    @Test
    void uniquenessIsOnTheCombination() {
        try (Engine e = ChronoDim.open(root.resolve("db"),
                EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(TableConfigIO.fromYaml(POSITION_YAML));

            // Same account twice, same symbol twice — but 3 distinct combinations.
            AuditManifest m1 = e.apply(ApplyBatch.single("L1", "position", List.of(
                    row("A1", "AAPL", 100L, "equities"),
                    row("A1", "MSFT", 50L, "equities"),
                    row("A2", "AAPL", 70L, "arb"))));
            assertEquals(3, m1.tables().get(0).inserts(), "3 combinations = 3 entities");

            // Re-sending an existing combination is an UPDATE of that entity —
            // not a duplicate, and not an update of anything sharing one column.
            AuditManifest m2 = e.apply(ApplyBatch.single("L2", "position", List.of(
                    row("A1", "AAPL", 120L, "equities"))));
            assertEquals(0, m2.tables().get(0).inserts());
            assertEquals(1, m2.tables().get(0).updates());

            // Unchanged values for an existing combination: a no-op, proving the
            // engine matched the entity by the full key and compared payloads.
            AuditManifest m3 = e.apply(ApplyBatch.single("L3", "position", List.of(
                    row("A1", "MSFT", 50L, "equities"))));
            assertEquals(1, m3.tables().get(0).noOps());

            // Point reads take the full combination; a partial/wrong combo is no entity.
            Version v = e.getCurrent("position", Map.of("account_id", "A1", "symbol", "AAPL")).orElseThrow();
            assertEquals(120L, v.row().get("qty"));
            assertTrue(e.getCurrent("position", Map.of("account_id", "A2", "symbol", "MSFT")).isEmpty());

            // History accrued only on the versioned entity; siblings untouched.
            // (3 records: new open version + superseding close + preserved original.)
            assertEquals(3, e.getHistory("position", Map.of("account_id", "A1", "symbol", "AAPL")).size());
            assertEquals(1, e.getHistory("position", Map.of("account_id", "A2", "symbol", "AAPL")).size());

            // Delete removes exactly one combination.
            Map<String, Object> del = new LinkedHashMap<>();
            del.put("account_id", "A1");
            del.put("symbol", "MSFT");
            e.apply(ApplyBatch.single("L4", "position", List.of(new InputRow(del, true))));
            List<String> current = new ArrayList<>();
            try (VersionCursor c = e.scanCurrent("position")) {
                while (c.hasNext()) {
                    Version cv = c.next();
                    current.add(cv.row().get("account_id") + "/" + cv.row().get("symbol"));
                }
            }
            assertEquals(List.of("A1/AAPL", "A2/AAPL"), current.stream().sorted().toList());
        }
    }

    @Test
    void nullInAnyKeyPartIsRejected() {
        try (Engine e = ChronoDim.open(root.resolve("db2"),
                EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(TableConfigIO.fromYaml(POSITION_YAML));

            AuditManifest m = e.apply(ApplyBatch.single("L1", "position", List.of(
                    row("A1", null, 10L, "equities"),   // symbol missing
                    row(null, "AAPL", 10L, "equities"), // account missing
                    row("A1", "AAPL", 10L, "equities"))));
            AuditManifest.TableStats st = m.tables().get(0);
            assertEquals(2, st.rejects(), "every key part is NOT NULL by definition");
            assertEquals(1, st.inserts());
            assertTrue(m.errors().stream().anyMatch(err -> err.reason().contains("symbol")));
            assertTrue(m.errors().stream().anyMatch(err -> err.reason().contains("account_id")));
        }
    }

    @Test
    void keyIsImmutableAndValidatedAtCreate() {
        try (Engine e = ChronoDim.open(root.resolve("db3"),
                EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(TableConfigIO.fromYaml(POSITION_YAML));

            // Changing the key would silently re-identify every entity — refused.
            TableConfig rekeyed = TableConfigIO.fromYaml(POSITION_YAML.replace(
                    "business_key: [account_id, symbol]", "business_key: [account_id]"));
            ConfigException ex = assertThrows(ConfigException.class, () -> e.alterTable(rekeyed));
            assertTrue(ex.getMessage().contains("business_key cannot change"));

            // Key columns must exist in the schema…
            assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("""
                    table: t
                    business_key: [nope]
                    schema:
                      - {name: id, type: string}
                    """));
            // …must be scalar (no array/map/struct keys), and a key is required.
            assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("""
                    table: t
                    business_key: [tags]
                    schema:
                      - {name: tags, type: "array<string>"}
                    """));
            assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("""
                    table: t
                    business_key: []
                    schema:
                      - {name: id, type: string}
                    """));
        }
    }
}
