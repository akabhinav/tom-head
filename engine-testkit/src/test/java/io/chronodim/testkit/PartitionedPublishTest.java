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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hive-style partitioned publishing: directory layout, exactly-once, partition-aware finalize. */
class PartitionedPublishTest {

    @TempDir
    Path root;

    private static EngineOptions opts() {
        return EngineOptions.builder().groupCommitWindowMicros(0).publishIntervalMillis(50).build();
    }

    private TableConfig table(Path location) {
        return TableConfigIO.fromYaml("""
                table: adj
                business_key: [adj_id]
                schema:
                  - {name: adj_id,     type: string}
                  - {name: region,     type: string}
                  - {name: year,       type: int}
                  - {name: amount,     type: "decimal(18,2)"}
                  - {name: effective,  type: timestamp}
                ignored_columns: [effective]
                valid_time:
                  mode: source_column
                  column: effective
                publish:
                  enabled: true
                  location: %s
                  partition_by: [region, year]
                """.formatted(location));
    }

    private static InputRow row(String id, String region, int year, String amount) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("adj_id", id);
        v.put("region", region);
        v.put("year", year);
        v.put("amount", amount);
        v.put("effective", "2026-0" + (1 + (id.hashCode() & 3)) + "-01T00:00:00Z");
        return InputRow.upsert(v);
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

    @Test
    void partitionedLayoutExactlyOnceAndFinalize() throws IOException {
        Path location = root.resolve("export");
        Path db = root.resolve("db");

        try (Engine e = ChronoDim.open(db, opts())) {
            e.createTable(table(location));
            e.apply(ApplyBatch.single("p1", "adj", List.of(
                    row("A1", "EU", 2025, "10.00"),
                    row("A2", "EU", 2026, "20.00"),
                    row("A3", "US", 2026, "30.00"),
                    row("A4", null, 2026, "40.00")))); // null partition value
            awaitPublished(e);
        }

        // Hive-style directories, null → __HIVE_DEFAULT_PARTITION__
        assertTrue(Files.isDirectory(location.resolve("data/region=EU/year=2025")));
        assertTrue(Files.isDirectory(location.resolve("data/region=EU/year=2026")));
        assertTrue(Files.isDirectory(location.resolve("data/region=US/year=2026")));
        assertTrue(Files.isDirectory(location.resolve("data/region=__HIVE_DEFAULT_PARTITION__/year=2026")));
        assertEquals(4, allRows(location).size());
        // Partition columns also stay in the row payload (glob readers keep working).
        for (Map<String, Object> r : allRows(location)) {
            assertTrue(r.containsKey("year"), r.toString());
        }
        // Contract advertises the partitioning.
        Map<String, Object> contract = Json.parseObject(Files.readString(location.resolve("_contract.json")));
        assertEquals(List.of("region", "year"), contract.get("partition_by"));

        // Restart: exactly-once (no duplicates), new commits land in the right partition.
        try (Engine e = ChronoDim.open(db, opts())) {
            assertEquals(4, allRows(location).size());
            e.apply(ApplyBatch.single("p2", "adj", List.of(row("A1", "EU", 2025, "11.00"))));
            awaitPublished(e);
        }
        // A1 update reuses the same valid_from → a superseding record (same instant,
        // newer belief) lands in region=EU/year=2025 as a second part.
        assertEquals(5, allRows(location).size());
        try (Stream<Path> walk = Files.walk(location.resolve("data/region=EU/year=2025"))) {
            long parts = walk.filter(p -> p.getFileName().toString().startsWith("part-")).count();
            assertEquals(2, parts);
        }

        // Partition-aware finalize: consolidated log + current snapshot per partition.
        Map<String, Object> result = Finalizer.run(location, List.of("adj_id"), List.of("region", "year"));
        assertEquals(5L, ((Number) result.get("finalized_rows")).longValue());
        assertEquals(4L, ((Number) result.get("current_rows")).longValue());
        assertTrue(Files.list(location.resolve("finalized/region=EU/year=2025"))
                .anyMatch(p -> p.getFileName().toString().startsWith("log-")));
        assertTrue(Files.list(location.resolve("finalized/region=US/year=2026"))
                .anyMatch(p -> p.getFileName().toString().startsWith("current-")));
        // Log preserved verbatim after finalize.
        assertEquals(5, allRows(location).size());
    }

    /** Every version record across data/ and finalized log files, recursively. */
    private static List<Map<String, Object>> allRows(Path location) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String sub : List.of("data", "finalized")) {
            Path dir = location.resolve(sub);
            if (!Files.isDirectory(dir)) continue;
            try (Stream<Path> walk = Files.walk(dir)) {
                for (Path p : walk.toList()) {
                    String name = p.getFileName().toString();
                    if (Files.isDirectory(p) || !name.endsWith(".jsonl") || name.startsWith("current-")) continue;
                    for (String line : Files.readAllLines(p)) {
                        if (!line.isBlank()) rows.add(Json.parseObject(line));
                    }
                }
            }
        }
        return rows;
    }
}
