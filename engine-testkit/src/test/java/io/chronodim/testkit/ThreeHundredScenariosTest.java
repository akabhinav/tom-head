package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The requested proof drill: a 2000-entity table, 300 randomized adjustment
 * scenarios of 300..600 rows each (updates, no-ops, deletes, re-inserts,
 * same-batch duplicates, late arrivals — all mixed), every scenario's
 * counters checked against the independent reference implementation, the FULL
 * state (current set + every key's complete history on both temporal axes)
 * compared at checkpoints, and the engine closed/reopened mid-soak and at the
 * end to prove WAL replay reproduces the identical state. Runs on both
 * storage backends = 600 scenarios total.
 */
class ThreeHundredScenariosTest {

    @TempDir
    Path root;

    private static final int SCENARIOS = 300;
    private static final int ENTITIES = 2000;
    private static final int CHECK_EVERY = 25;

    @ParameterizedTest
    @ValueSource(strings = {"ROCKSDB", "LSM"})
    void threeHundredRandomScenariosMatchReferenceExactly(String backend) {
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(4242 + backend.hashCode(),
                new DataGen.StreamOptions(ENTITIES, 0.15, 0.07, 0.05, 0.2));
        Random rnd = new Random(99 + backend.hashCode());
        EngineOptions opts = EngineOptions.builder()
                .storageBackend(EngineOptions.StorageBackend.valueOf(backend))
                .groupCommitWindowMicros(0)
                .publishEnabled(false)
                .build();
        Path dir = root.resolve("db-" + backend);

        Engine e = ChronoDim.open(dir, opts);
        try {
            e.createTable(cfg);
            long totalRows = 0;
            for (int s = 0; s < SCENARIOS; s++) {
                int n = 300 + rnd.nextInt(301); // 300..600 adjustments
                List<InputRow> rows = gen.nextBatch(n);
                AuditManifest m = e.apply(ApplyBatch.single("scenario-" + backend + "-" + s, "t", rows));
                Map<String, Long> exp = ref.applyBatch(rows, m.startedAtMicros());
                AuditManifest.TableStats st = m.tables().get(0);
                assertEquals(exp.get("inserts"), st.inserts(), "inserts, scenario " + s);
                assertEquals(exp.get("updates"), st.updates(), "updates, scenario " + s);
                assertEquals(exp.get("no_ops"), st.noOps(), "no_ops, scenario " + s);
                assertEquals(exp.get("deletes"), st.deletes(), "deletes, scenario " + s);
                assertEquals(exp.get("late_splits"), st.lateSplits(), "late_splits, scenario " + s);
                totalRows += n;

                if ((s + 1) % CHECK_EVERY == 0) {
                    DifferentialTest.compareFullState(e, ref, cfg); // full state, both axes
                }
                if (s == SCENARIOS / 2) { // mid-soak restart: WAL replay must be exact
                    e.close();
                    e = ChronoDim.open(dir, opts);
                    DifferentialTest.compareFullState(e, ref, cfg);
                }
            }
            DifferentialTest.compareFullState(e, ref, cfg);
            System.out.printf("%s: %d scenarios, %d adjustment rows, full-state checks passed%n",
                    backend, SCENARIOS, totalRows);
        } finally {
            e.close();
        }

        // Final restart: the whole soak replays to the identical state.
        try (Engine e2 = ChronoDim.open(dir, opts)) {
            DifferentialTest.compareFullState(e2, ref, cfg);
        }
    }
}
