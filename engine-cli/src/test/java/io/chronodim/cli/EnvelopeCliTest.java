package io.chronodim.cli;

import io.chronodim.api.InputRow;
import io.chronodim.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The metadata envelope: one JSON file = audit metadata + records, end to end through the CLI. */
class EnvelopeCliTest {

    @TempDir
    Path root;

    @Test
    void envelopeParsing() throws IOException {
        Path f = root.resolve("adj.json");
        Files.writeString(f, """
                {
                  "table": "customer",
                  "load_id": "adj-Q2-0417",
                  "effective_at": "2026-06-30T00:00:00Z",
                  "source": "finance-ops",
                  "metadata": {"approved_by": "jsmith"},
                  "defaults": {"segment": "RETAIL"},
                  "records": [
                    {"customer_id": "C1", "risk": 10},
                    {"customer_id": "C2", "risk": 20, "segment": "SME", "_op": "delete"}
                  ]
                }
                """);
        RowFileReader.ParsedInput in = RowFileReader.read(f, null, null);
        assertEquals("adj-Q2-0417", in.loadId());
        assertEquals("2026-06-30T00:00:00Z", in.effectiveAt());
        // both nested metadata and free top-level keys are captured
        assertEquals("jsmith", in.metadata().get("approved_by"));
        assertEquals("finance-ops", in.metadata().get("source"));
        List<InputRow> rows = in.byTable().get("customer");
        assertEquals(2, rows.size());
        assertEquals("RETAIL", rows.get(0).values().get("segment"), "defaults merged");
        assertEquals("SME", rows.get(1).values().get("segment"), "record wins over defaults");
        assertTrue(rows.get(1).delete());
    }

    @Test
    void envelopeDrivesTheWholeApply(@TempDir Path dir) throws IOException {
        Path cfg = dir.resolve("t.yaml");
        Files.writeString(cfg, """
                table: customer
                business_key: [customer_id]
                schema:
                  - {name: customer_id, type: string}
                  - {name: risk,        type: long}
                  - {name: segment,     type: string}
                """);
        Path db = dir.resolve("db");
        assertEquals(0, cmd("table", "create", "-d", db.toString(), "--no-publish", "-f", cfg.toString()));

        Path envelope = dir.resolve("adj.json");
        Files.writeString(envelope, """
                {
                  "table": "customer",
                  "load_id": "env-1",
                  "effective_at": "2026-06-30T00:00:00Z",
                  "approved_by": "jsmith",
                  "records": [
                    {"customer_id": "C1", "risk": 10, "segment": "RETAIL"},
                    {"customer_id": "C2", "risk": 20, "segment": "SME"}
                  ]
                }
                """);
        // No --table, no --load-id: the envelope carries everything.
        assertEquals(0, cmd("apply", envelope.toString(), "-d", db.toString(), "--no-publish"));
        // Idempotent replay via the envelope's load_id.
        assertEquals(0, cmd("apply", envelope.toString(), "-d", db.toString(), "--no-publish"));

        // effective_at became the valid time; metadata is in the manifest.
        assertEquals(0, cmd("asof", "-d", db.toString(), "--no-publish", "-t", "customer",
                "--valid-time", "2026-07-01T00:00:00Z", "customer_id=C1"));
        assertEquals(3, cmd("asof", "-d", db.toString(), "--no-publish", "-t", "customer",
                "--valid-time", "2026-06-29T00:00:00Z", "customer_id=C1"), "before effective_at → not found");
        assertEquals(0, cmd("manifest", "show", "env-1", "-d", db.toString(), "--no-publish"));
    }

    @Test
    void conflictingLoadIdsAreRejected() throws IOException {
        Path f = root.resolve("e.json");
        Files.writeString(f, "{\"table\": \"t\", \"load_id\": \"a\", \"records\": []}");
        RowFileReader.ParsedInput in = RowFileReader.read(f, null, null);
        Main.IngestOpts ingest = new Main.IngestOpts();
        ingest.loadId = "b";
        assertThrows(ValidationException.class, () -> ingest.resolveLoadId(in.loadId()));
        ingest.loadId = "a";
        assertEquals("a", ingest.resolveLoadId(in.loadId()));
        ingest.loadId = null;
        assertEquals("a", ingest.resolveLoadId(in.loadId()));
    }

    private static int cmd(String... args) {
        return new CommandLine(new Main()).setExecutionExceptionHandler((ex, c, p) -> {
            System.err.println("cli error: " + ex);
            return ex instanceof ValidationException ? 2 : 1;
        }).execute(args);
    }
}
