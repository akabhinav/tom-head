package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.core.catalog.TableConfigIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * absent_columns: carry_forward — a partial adjustment (only some columns
 * sent) inherits every missing column from the IMMEDIATE previous version;
 * explicit null still clears; unchanged partial resends are no-ops; the
 * default (NULL) semantics are untouched.
 */
class PartialAdjustmentTest {

    @TempDir
    Path root;

    private static final String YAML = """
            table: c
            business_key: [id]
            absent_columns: carry_forward
            schema:
              - {name: id,   type: string}
              - {name: name, type: string}
              - {name: seg,  type: string}
              - {name: risk, type: double}
              - {name: eff,  type: timestamp}
            valid_time: {mode: source_column, column: eff}
            quality_gates:
              - {column: name, rule: not_null}
            """;

    private static InputRow partial(String eff, Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", "C1");
        m.put("eff", eff);
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return InputRow.upsert(m);
    }

    @Test
    void carryForwardInheritsFromImmediatePrevious() {
        try (Engine e = ChronoDim.open(root.resolve("db"), EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(TableConfigIO.fromYaml(YAML));
            e.apply(ApplyBatch.single("L1", "c", List.of(
                    partial("2026-01-01T00:00:00Z", "name", "Asha", "seg", "RETAIL", "risk", 210.0))));

            // Adjust ONLY risk: name/seg inherited. Gate on absent 'name' must not fire.
            assertEquals(1, e.apply(ApplyBatch.single("L2", "c", List.of(
                    partial("2026-02-01T00:00:00Z", "risk", 260.0)))).tables().get(0).updates());
            Map<String, Object> r2 = e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row();
            assertEquals("Asha", r2.get("name"));
            assertEquals("RETAIL", r2.get("seg"));
            assertEquals(260.0, r2.get("risk"));

            // Adjust ONLY seg: risk must come from the IMMEDIATE previous (260), not v1's 210.
            e.apply(ApplyBatch.single("L3", "c", List.of(partial("2026-03-01T00:00:00Z", "seg", "SME"))));
            Map<String, Object> r3 = e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row();
            assertEquals(260.0, r3.get("risk"), "must inherit from immediate previous version");
            assertEquals("SME", r3.get("seg"));

            // Identical partial resend = no-op (hash recomputed over the merged row).
            assertEquals(1, e.apply(ApplyBatch.single("L4", "c", List.of(
                    partial("2026-03-01T00:00:00Z", "seg", "SME")))).tables().get(0).noOps());

            // Explicit null clears deliberately (absent != null).
            Map<String, Object> m = new HashMap<>();
            m.put("id", "C1");
            m.put("eff", "2026-04-01T00:00:00Z");
            m.put("seg", null);
            e.apply(ApplyBatch.single("L5", "c", List.of(InputRow.upsert(m))));
            assertNull(e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row().get("seg"));
            assertEquals("Asha", e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row().get("name"));

            // History intact: 4 latest-belief intervals with the right values.
            assertEquals(4, e.getHistory("c", Map.of("id", "C1")).stream()
                    .map(v -> v.validFrom()).distinct().count());
        }
        // Reopen: WAL replay reproduces the merged rows identically.
        try (Engine e = ChronoDim.open(root.resolve("db"), EngineOptions.builder().publishEnabled(false).build())) {
            assertEquals(260.0, e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row().get("risk"));
        }
    }

    @Test
    void defaultTablesKeepFullRowSemantics() {
        try (Engine e = ChronoDim.open(root.resolve("db2"), EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(TableConfigIO.fromYaml(YAML.replace("absent_columns: carry_forward\n", "")
                    .replace("quality_gates:\n  - {column: name, rule: not_null}\n", "")));
            e.apply(ApplyBatch.single("L1", "c", List.of(
                    partial("2026-01-01T00:00:00Z", "name", "Asha", "seg", "RETAIL", "risk", 210.0))));
            e.apply(ApplyBatch.single("L2", "c", List.of(partial("2026-02-01T00:00:00Z", "risk", 260.0))));
            assertNull(e.getCurrent("c", Map.of("id", "C1")).orElseThrow().row().get("name"),
                    "NULL semantics unchanged: absent column becomes null");
        }
    }
}
