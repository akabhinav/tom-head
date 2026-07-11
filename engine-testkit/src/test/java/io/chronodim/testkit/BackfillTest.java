package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bulk backfill (R-APPLY-8): sorted-ingest path must produce the same state as apply. */
class BackfillTest {

    @TempDir
    Path root;

    private static EngineOptions opts() {
        return EngineOptions.builder().groupCommitWindowMicros(0).build();
    }

    @Test
    void backfillMatchesReferenceThenIncrementalsApply() {
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(5, DataGen.StreamOptions.defaults());

        // One large initial load (with history per key, duplicates, deletes).
        List<InputRow> initial = new ArrayList<>();
        for (int i = 0; i < 6; i++) initial.addAll(gen.nextBatch(40));

        try (Engine e = ChronoDim.open(root.resolve("db"), opts())) {
            e.createTable(cfg);
            AuditManifest m = e.backfill("bulk-1", "t", initial);
            assertTrue(m.backfill());
            ref.applyBatch(initial, m.startedAtMicros());
            DifferentialTest.compareFullState(e, ref, cfg);

            // Idempotent: same load id returns the original manifest, no re-ingest.
            AuditManifest again = e.backfill("bulk-1", "t", initial);
            assertTrue(again.alreadyApplied());
            assertEquals(m.txnId(), again.txnId());

            // A second backfill into the non-empty table is refused.
            assertThrows(ValidationException.class, () -> e.backfill("bulk-2", "t", initial));

            // Incremental CDC on top of the ingested state.
            for (int b = 0; b < 5; b++) {
                List<InputRow> rows = gen.nextBatch(30);
                AuditManifest am = e.apply(ApplyBatch.single("inc-" + b, "t", rows));
                ref.applyBatch(rows, am.startedAtMicros());
            }
            DifferentialTest.compareFullState(e, ref, cfg);
        }

        // Recovery after restart still matches (backfill fence + storage watermark).
        try (Engine e = ChronoDim.open(root.resolve("db"), opts())) {
            DifferentialTest.compareFullState(e, ref, cfg);
        }
    }
}
