package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.core.catalog.TableConfigIO;
import io.chronodim.core.util.Json;
import io.chronodim.export.Finalizer;
import io.chronodim.export.PublishedContract;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Databricks-convention publishing: __START_AT/__END_AT (END null = active), no
 * operation column, deletes = closed final version with no successor. Column
 * names are per-table configurable because every team's template differs.
 */
class DatabricksStyleTest {

    @TempDir
    Path root;

    private static EngineOptions opts() {
        return EngineOptions.builder().groupCommitWindowMicros(0).publishIntervalMillis(50).build();
    }

    private static InputRow row(String id, String v, String ts) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("v", v);
        m.put("ts", ts);
        return InputRow.upsert(m);
    }

    private static void awaitPublished(Engine e) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Object lag = e.stats().get("publish_lag_txns");
            if (lag != null && ((Number) lag).longValue() == 0) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        throw new AssertionError("publisher lagging: " + e.stats());
    }

    private static List<Map<String, Object>> rows(Path location, String dir, String prefix) throws IOException {
        List<Map<String, Object>> out = new ArrayList<>();
        Path d = location.resolve(dir);
        if (!Files.isDirectory(d)) return out;
        try (Stream<Path> walk = Files.walk(d)) {
            for (Path p : walk.toList()) {
                String n = p.getFileName().toString();
                if (Files.isDirectory(p) || !n.endsWith(".jsonl") || !n.startsWith(prefix)) continue;
                for (String line : Files.readAllLines(p)) {
                    if (!line.isBlank()) out.add(Json.parseObject(line));
                }
            }
        }
        return out;
    }

    @Test
    void databricksShapeEndToEnd() throws IOException {
        Path location = root.resolve("export");
        TableConfig cfg = TableConfigIO.fromYaml("""
                table: dim
                business_key: [id]
                schema:
                  - {name: id, type: string}
                  - {name: v,  type: string}
                  - {name: ts, type: timestamp}
                ignored_columns: [ts]
                valid_time: {mode: source_column, column: ts}
                publish:
                  enabled: true
                  location: %s
                  column_style: databricks
                """.formatted(location));

        try (Engine e = ChronoDim.open(root.resolve("db"), opts())) {
            e.createTable(cfg);
            e.apply(ApplyBatch.single("d1", "dim", List.of(
                    row("A", "v1", "2026-01-01T00:00:00Z"),
                    row("B", "v1", "2026-01-01T00:00:00Z"))));
            e.apply(ApplyBatch.single("d2", "dim", List.of(row("A", "v2", "2026-03-01T00:00:00Z"))));
            Map<String, Object> del = new LinkedHashMap<>();
            del.put("id", "B");
            del.put("ts", "2026-04-01T00:00:00Z");
            e.apply(ApplyBatch.single("d3", "dim", List.of(new InputRow(del, true))));
            awaitPublished(e);
        }

        List<Map<String, Object>> published = rows(location, "data", "part-");
        // d1: A/v1 + B/v1; d2: A close-rewrite + A/v2; d3: B close-rewrite only (NO tombstone row)
        assertEquals(5, published.size(), published.toString());
        for (Map<String, Object> r : published) {
            assertTrue(r.containsKey("__START_AT"), r.toString());
            assertTrue(r.containsKey("__END_AT"), r.toString());
            assertTrue(!r.containsKey("_op") && !r.containsKey("_valid_from") && !r.containsKey("_schema_version"), r.toString());
        }
        // Active rows: exactly one (A/v2 with END null); B has no active row after its delete.
        Map<String, Map<String, Object>> latestPerKeyStart = new LinkedHashMap<>();
        for (Map<String, Object> r : published) {
            String k = r.get("id") + "|" + r.get("__START_AT");
            Map<String, Object> prev = latestPerKeyStart.get(k);
            if (prev == null || String.valueOf(r.get("_tx_time")).compareTo(String.valueOf(prev.get("_tx_time"))) > 0) {
                latestPerKeyStart.put(k, r);
            }
        }
        List<Map<String, Object>> active = latestPerKeyStart.values().stream()
                .filter(r -> r.get("__END_AT") == null).toList();
        assertEquals(1, active.size(), active.toString());
        assertEquals("A", active.get(0).get("id"));
        assertEquals("v2", active.get(0).get("v"));
        // B's final belief: closed at the delete instant.
        assertEquals("2026-04-01T00:00:00Z",
                latestPerKeyStart.get("B|2026-01-01T00:00:00Z").get("__END_AT"));

        // View template speaks the team's columns.
        String view = Files.readString(location.resolve("scd2_view.sql"));
        assertTrue(view.contains("__START_AT") && view.contains("__END_AT") && !view.contains("_op"), view);
        Map<String, Object> contract = Json.parseObject(Files.readString(location.resolve("_contract.json")));
        assertEquals("DATABRICKS", contract.get("column_style"));

        // Finalize keeps the shape: current snapshot = rows with __END_AT null.
        Map<String, Object> result = Finalizer.run(location, List.of("id"), List.of(), PublishedContract.Style.DATABRICKS);
        assertEquals(1L, ((Number) result.get("current_rows")).longValue());
        List<Map<String, Object>> current = rows(location, "finalized", "current-");
        assertEquals(1, current.size());
        assertEquals("A", current.get(0).get("id"));
        assertNull(current.get(0).get("__END_AT"));
    }

    @Test
    void customTeamTemplateColumns() throws IOException {
        Path location = root.resolve("export2");
        TableConfig cfg = TableConfigIO.fromYaml("""
                table: dim2
                business_key: [id]
                schema:
                  - {name: id, type: string}
                  - {name: v,  type: string}
                  - {name: ts, type: timestamp}
                ignored_columns: [ts]
                valid_time: {mode: source_column, column: ts}
                publish:
                  enabled: true
                  location: %s
                  scd2_columns:
                    start: EFF_START_DT
                    end: EFF_END_DT
                    include_ops: false
                """.formatted(location));

        try (Engine e = ChronoDim.open(root.resolve("db2"), opts())) {
            e.createTable(cfg);
            e.apply(ApplyBatch.single("c1", "dim2", List.of(row("X", "v1", "2026-01-01T00:00:00Z"))));
            awaitPublished(e);
        }
        List<Map<String, Object>> published = rows(location, "data", "part-");
        assertEquals(1, published.size());
        assertTrue(published.get(0).containsKey("EFF_START_DT"), published.toString());
        assertTrue(published.get(0).containsKey("EFF_END_DT"), published.toString());
        assertTrue(!published.get(0).containsKey("_op"), published.toString());
        Map<String, Object> contract = Json.parseObject(Files.readString(location.resolve("_contract.json")));
        assertEquals("CUSTOM", contract.get("column_style"));
    }
}
