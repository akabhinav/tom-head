package io.chronodim.cli;

import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bulk `load` command must STREAM file rows, not materialize the file
 * (a 2 GB wide-row JSONL OOM'd the old path). These tests pin the streaming
 * behavior end to end through the CLI for jsonl and csv, and the json
 * envelope fallback (single-document formats still read whole, by design).
 */
class LoadStreamTest {

    @TempDir
    Path root;

    private int cli(String... args) {
        return new CommandLine(new Main()).execute(args);
    }

    private Path table() throws IOException {
        Path yaml = root.resolve("t.yaml");
        Files.writeString(yaml, """
                table: t
                business_key: [id]
                schema:
                  - {name: id, type: string}
                  - {name: v,  type: long}
                """);
        return yaml;
    }

    @Test
    void jsonlLoadStreamsInBoundedMemory() throws Exception {
        Path data = root.resolve("db");
        assertEquals(0, cli("table", "create", "-f", table().toString(), "-d", data.toString()));

        // 60k rows through a tiny 32 MB heap: impossible if the file is
        // materialized as maps, trivial when streamed.
        Path jsonl = root.resolve("bulk.jsonl");
        try (var w = Files.newBufferedWriter(jsonl)) {
            for (int i = 0; i < 60_000; i++) {
                w.write("{\"id\":\"K" + i + "\",\"v\":" + i + ",\"pad\":\"" + "x".repeat(200) + "\"}\n");
            }
        }
        Process p = new ProcessBuilder(
                System.getProperty("java.home") + "/bin/java", "-Xmx32m",
                "-cp", System.getProperty("java.class.path"),
                "io.chronodim.cli.Main", "load", jsonl.toString(),
                "-d", data.toString(), "-t", "t", "--load-id", "big", "--json")
                .redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertEquals(0, p.waitFor(), out);
        assertTrue(out.contains("\"rows_in\":60000") || out.contains("\"rows_in\": 60000"), out);

        try (Engine e = ChronoDim.open(data, EngineOptions.builder().publishEnabled(false).build())) {
            assertEquals(59_999L, e.getCurrent("t", Map.of("id", "K59999")).orElseThrow().row().get("v"));
        }
    }

    @Test
    void csvAndEnvelopeLoadStillWork() throws Exception {
        Path data = root.resolve("db2");
        assertEquals(0, cli("table", "create", "-f", table().toString(), "-d", data.toString()));

        Path csv = root.resolve("bulk.csv");
        Files.writeString(csv, "id,v\nA,1\nB,2\n");
        assertEquals(0, cli("load", csv.toString(), "-d", data.toString(), "-t", "t", "--load-id", "c1"));

        try (Engine e = ChronoDim.open(data, EngineOptions.builder().publishEnabled(false).build())) {
            assertEquals(2L, e.getCurrent("t", Map.of("id", "B")).orElseThrow().row().get("v"));
        }

        // json envelope path (whole-document by design): table + load_id from the file.
        Path data3 = root.resolve("db3");
        assertEquals(0, cli("table", "create", "-f", table().toString(), "-d", data3.toString()));
        Path env = root.resolve("env.json");
        Files.writeString(env, "{\"table\":\"t\",\"load_id\":\"e1\",\"records\":[{\"id\":\"Z\",\"v\":9}]}");
        assertEquals(0, cli("load", env.toString(), "-d", data3.toString()));
        try (Engine e = ChronoDim.open(data3, EngineOptions.builder().publishEnabled(false).build())) {
            assertEquals(9L, e.getCurrent("t", Map.of("id", "Z")).orElseThrow().row().get("v"));
        }
    }
}
