package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableConfigIO;
import io.chronodim.core.util.Json;
import io.chronodim.export.Finalizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Publisher (R-PUB): frozen contract, exactly-once across restarts, finalizer union. */
class PublishTest {

    @TempDir
    Path root;

    private TableConfig publishedTable(Path location) {
        return TableConfigIO.fromYaml("""
                table: pub
                business_key: [id]
                schema:
                  - {name: id,         type: string}
                  - {name: value,      type: string}
                  - {name: updated_at, type: timestamp}
                ignored_columns: [updated_at]
                valid_time:
                  mode: source_column
                  column: updated_at
                publish:
                  enabled: true
                  location: %s
                """.formatted(location));
    }

    private static InputRow row(String id, String value, String ts) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        v.put("value", value);
        v.put("updated_at", ts);
        return InputRow.upsert(v);
    }

    private static EngineOptions opts() {
        return EngineOptions.builder().groupCommitWindowMicros(0).publishIntervalMillis(50).build();
    }

    private static void awaitPublished(Engine e) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Object lag = e.stats().get("publish_lag_txns");
            if (lag != null && ((Number) lag).longValue() == 0) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("publisher did not catch up within 10s: " + e.stats());
    }

    private static List<Map<String, Object>> publishedRows(Path location) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String sub : List.of("data", "finalized")) {
            Path dir = location.resolve(sub);
            if (!Files.isDirectory(dir)) continue;
            try (var ds = Files.newDirectoryStream(dir, "*.jsonl")) {
                for (Path p : ds) {
                    if (sub.equals("finalized") && p.getFileName().toString().startsWith("current-")) continue;
                    for (String line : Files.readAllLines(p)) {
                        if (!line.isBlank()) rows.add(Json.parseObject(line));
                    }
                }
            }
        }
        return rows;
    }

    @Test
    void publishesEveryVersionExactlyOnceAcrossRestarts() throws Exception {
        Path location = root.resolve("export");
        Path db = root.resolve("db");

        try (Engine e = ChronoDim.open(db, opts())) {
            e.createTable(publishedTable(location));
            e.apply(ApplyBatch.single("p1", "pub", List.of(
                    row("A", "v1", "2024-01-01T00:00:00Z"),
                    row("B", "v1", "2024-01-01T00:00:00Z"))));
            e.apply(ApplyBatch.single("p2", "pub", List.of(
                    row("A", "v2", "2024-02-01T00:00:00Z")))); // closes A/v1 + inserts A/v2
            awaitPublished(e);
        }

        List<Map<String, Object>> rows = publishedRows(location);
        // p1: 2 inserts; p2: 1 close-rewrite + 1 new = 4 version records total
        assertEquals(4, rows.size(), "published rows: " + rows);
        assertTrue(Files.exists(location.resolve("scd2_view.sql")));
        assertTrue(Files.exists(location.resolve("_contract.json")));
        for (Map<String, Object> r : rows) {
            assertTrue(r.containsKey("_valid_from") && r.containsKey("_txn_id") && r.containsKey("_op"), r.toString());
        }

        // Restart: nothing must be re-published (R-PUB-2), new commits continue the log.
        try (Engine e = ChronoDim.open(db, opts())) {
            assertEquals(4, publishedRows(location).size(), "restart must not re-publish");
            e.apply(ApplyBatch.single("p3", "pub", List.of(row("B", "v2", "2024-03-01T00:00:00Z"))));
            awaitPublished(e);
        }
        assertEquals(6, publishedRows(location).size()); // close B/v1 + new B/v2

        // Orphan part (crash between part write and log write) is swept on attach.
        Path orphan = location.resolve("data/part-99999999999999999999-99999999999999999999.jsonl");
        Files.writeString(orphan, "{\"junk\": true}\n");
        try (Engine e = ChronoDim.open(db, opts())) {
            assertTrue(!Files.exists(orphan), "orphan part must be removed on attach");
            assertEquals(6, publishedRows(location).size());
        }
    }

    @Test
    void finalizerMaterializesCurrentAndPreservesLog() throws Exception {
        Path location = root.resolve("export2");
        Path db = root.resolve("db2");
        Map<String, String> expectCurrent = new HashMap<>();

        try (Engine e = ChronoDim.open(db, opts())) {
            e.createTable(publishedTable(location));
            for (int b = 0; b < 5; b++) {
                List<InputRow> rows = new ArrayList<>();
                for (int k = 0; k < 6; k++) {
                    String val = "v" + b + "." + k;
                    rows.add(row("K" + k, val, "2024-0" + (b + 1) + "-01T00:00:00Z"));
                    expectCurrent.put("K" + k, val);
                }
                e.apply(ApplyBatch.single("f" + b, "pub", rows));
            }
            awaitPublished(e);
            long before = publishedRows(location).size();

            Map<String, Object> result = Finalizer.run(location, List.of("id"));
            assertEquals(before, ((Number) result.get("finalized_rows")).longValue(), "log preserved verbatim");
            assertEquals(6L, ((Number) result.get("current_rows")).longValue());

            // Consolidated log still holds every record; current snapshot matches engine state.
            assertEquals(before, publishedRows(location).size());
            List<Map<String, Object>> current = new ArrayList<>();
            try (var ds = Files.newDirectoryStream(location.resolve("finalized"), "current-*.jsonl")) {
                for (Path p : ds) {
                    for (String line : Files.readAllLines(p)) {
                        if (!line.isBlank()) current.add(Json.parseObject(line));
                    }
                }
            }
            assertEquals(6, current.size());
            Map<String, String> engineCurrent = new HashMap<>();
            try (VersionCursor c = e.scanCurrent("pub")) {
                while (c.hasNext()) {
                    Version v = c.next();
                    engineCurrent.put((String) v.row().get("id"), (String) v.row().get("value"));
                }
            }
            for (Map<String, Object> r : current) {
                assertEquals(engineCurrent.get(String.valueOf(r.get("id"))), r.get("value"), r.toString());
                assertEquals(Boolean.TRUE, r.get("_is_current"));
            }
            assertEquals(expectCurrent, engineCurrent);

            // Publishing continues after finalize; the union stays consistent.
            e.apply(ApplyBatch.single("post-final", "pub", List.of(row("K0", "after", "2024-09-01T00:00:00Z"))));
            awaitPublished(e);
            assertTrue(publishedRows(location).size() > before);
        }
    }
}
