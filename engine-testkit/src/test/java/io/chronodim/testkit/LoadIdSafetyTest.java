package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.RunId;
import io.chronodim.api.ValidationException;
import io.chronodim.core.catalog.TableConfigIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The run/transaction-id safety contract: reusing an id with the SAME batch is
 * an idempotent retry; reusing it with DIFFERENT content is refused loudly and
 * applies nothing; numeric (BIGINT) ids can be enforced; RunId mints unique,
 * time-ordered BIGINTs at any rate.
 */
class LoadIdSafetyTest {

    @TempDir
    Path root;

    private static void createTable(Engine e) {
        e.createTable(TableConfigIO.fromYaml("""
                table: t
                business_key: [id]
                schema:
                  - {name: id, type: string}
                  - {name: v,  type: long}
                """));
    }

    private static InputRow row(String id, long v) {
        return InputRow.upsert(Map.of("id", id, "v", v));
    }

    @Test
    void sameContentRetriesButDifferentContentIsRefused() {
        try (Engine e = ChronoDim.open(root.resolve("db"), EngineOptions.builder().publishEnabled(false).build())) {
            createTable(e);
            AuditManifest first = e.apply(ApplyBatch.single("run-100", "t", List.of(row("A", 1))));
            assertEquals(false, first.alreadyApplied());

            // Retry with the identical batch: the original receipt, nothing applied.
            AuditManifest retry = e.apply(ApplyBatch.single("run-100", "t", List.of(row("A", 1))));
            assertTrue(retry.alreadyApplied());
            assertEquals(first.txnId(), retry.txnId());

            // Same id, DIFFERENT content: refused loudly, nothing applied.
            ValidationException ex = assertThrows(ValidationException.class,
                    () -> e.apply(ApplyBatch.single("run-100", "t", List.of(row("A", 999)))));
            assertTrue(ex.getMessage().contains("DIFFERENT content"), ex.getMessage());
            assertEquals(1L, e.getCurrent("t", Map.of("id", "A")).orElseThrow().row().get("v"),
                    "the reused id's data must never land");
            assertEquals(1, e.getHistory("t", Map.of("id", "A")).size());

            // The engine keeps working normally afterwards.
            assertEquals(1, e.apply(ApplyBatch.single("run-101", "t", List.of(row("A", 2))))
                    .tables().get(0).updates());
        }
    }

    @Test
    void numericModeEnforcesBigintIds() {
        try (Engine e = ChronoDim.open(root.resolve("db2"),
                EngineOptions.builder().publishEnabled(false).numericLoadIds(true).build())) {
            createTable(e);
            for (String bad : new String[]{"abc", "run-1", "-5", "0", "1.5", "99999999999999999999", ""}) {
                assertThrows(ValidationException.class,
                        () -> e.apply(ApplyBatch.single(bad, "t", List.of(row("N", 1)))),
                        "should reject '" + bad + "'");
            }
            assertEquals(1, e.apply(ApplyBatch.single("9223372036854775807", "t", List.of(row("N", 1))))
                    .tables().get(0).inserts());
            assertEquals(1, e.apply(ApplyBatch.single(String.valueOf(RunId.next()), "t", List.of(row("N", 2))))
                    .tables().get(0).updates());
        }
    }

    @Test
    void runIdsAreUniqueOrderedBigints() throws Exception {
        int perThread = 50_000;
        int threads = 4;
        Set<Long> all = java.util.Collections.synchronizedSet(new HashSet<>());
        List<Thread> ts = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            ts.add(Thread.ofPlatform().start(() -> {
                long prev = 0;
                for (int i = 0; i < perThread; i++) {
                    long id = RunId.next();
                    assertTrue(id > 0, "must be a positive BIGINT");
                    assertTrue(id > prev, "must be strictly increasing per thread");
                    prev = id;
                    all.add(id);
                }
            }));
        }
        for (Thread t : ts) t.join(60_000);
        assertEquals(threads * perThread, all.size(), "no duplicates across 200k concurrent mints");
    }
}
