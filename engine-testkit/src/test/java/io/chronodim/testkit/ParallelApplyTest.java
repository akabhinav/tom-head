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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The sharded apply path (R-PERF-2) activates above ~2048 entities per batch.
 * Same differential guarantee as the small-batch tests: full state must equal
 * the single-threaded reference implementation, on both storage backends,
 * including late arrivals, deletes, duplicates and re-sends inside big batches.
 */
class ParallelApplyTest {

    @TempDir
    Path root;

    @ParameterizedTest
    @ValueSource(strings = {"ROCKSDB", "LSM"})
    void largeBatchesMatchReference(String backend) {
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        EngineOptions opts = EngineOptions.builder()
                .storageBackend(EngineOptions.StorageBackend.valueOf(backend))
                .groupCommitWindowMicros(0)
                .publishEnabled(false)
                .build();
        Random rnd = new Random(77);

        try (Engine e = ChronoDim.open(root.resolve("db-" + backend), opts)) {
            e.createTable(cfg);
            long ts = io.chronodim.api.ColumnType.parseTimestampMicros("2026-01-01T00:00:00Z");

            for (int b = 0; b < 4; b++) {
                // ~6000 entities per batch → well past the sharding threshold.
                List<InputRow> rows = new ArrayList<>();
                for (int k = 0; k < 6000; k++) {
                    String id = "K" + k;
                    double p = rnd.nextDouble();
                    Map<String, Object> v = new LinkedHashMap<>();
                    v.put("customer_id", id);
                    if (b > 0 && p < 0.05) {
                        v.put("updated_at", ts + (long) b * 1_000_000_000L + k);
                        rows.add(new InputRow(v, true)); // delete
                        continue;
                    }
                    boolean noChange = b > 0 && p < 0.35;
                    int salt = noChange ? 0 : b; // same values as batch 0 → no-op
                    v.put("name", "N" + k + "-" + salt);
                    v.put("segment", List.of("RETAIL", "SME", "CORP", "PRIVATE").get((k + salt) % 4));
                    v.put("risk_score", (double) ((k * 31 + salt * 7) % 1000));
                    v.put("exposure", java.math.BigDecimal.valueOf((k % 100000) * 100L + salt, 4));
                    v.put("active", (k + salt) % 2 == 0);
                    v.put("onboarded", "2020-01-0" + (1 + k % 9));
                    v.put("score_count", (long) (k % 500));
                    v.put("token", new byte[]{(byte) k, (byte) salt});
                    v.put("ratio", (k % 97) / 8f);
                    v.put("branch_no", k % 9999);
                    v.put("tags", List.of("t" + (k % 5)));
                    Map<String, Object> addr = new LinkedHashMap<>();
                    addr.put("city", "City" + (k % 20));
                    addr.put("zip", String.valueOf(1000 + k % 9000));
                    v.put("address", addr);
                    Map<String, Object> limits = new LinkedHashMap<>();
                    limits.put("daily", (long) (k % 100000));
                    v.put("limits", limits);
                    long vf = b > 0 && p >= 0.90
                            ? ts - 86_400_000_000L * (1 + k % 20)          // late arrival
                            : ts + (long) b * 1_000_000_000L + k;
                    v.put("updated_at", vf);
                    v.put("noise", "n" + rnd.nextInt());
                    rows.add(InputRow.upsert(v));
                }
                AuditManifest m = e.apply(ApplyBatch.single("big-" + b, "t", rows));
                Map<String, Long> refCounters = ref.applyBatch(rows, m.startedAtMicros());

                // Counters must agree exactly with the sequential reference.
                AuditManifest.TableStats st = m.tables().get(0);
                assertEquals(refCounters.get("inserts"), st.inserts(), "inserts batch " + b);
                assertEquals(refCounters.get("updates"), st.updates(), "updates batch " + b);
                assertEquals(refCounters.get("no_ops"), st.noOps(), "no_ops batch " + b);
                assertEquals(refCounters.get("deletes"), st.deletes(), "deletes batch " + b);
                assertEquals(refCounters.get("late_splits"), st.lateSplits(), "late_splits batch " + b);
            }
            DifferentialTest.compareFullState(e, ref, cfg);
        }

        // Recovery after sharded batches replays identically.
        try (Engine e = ChronoDim.open(root.resolve("db-" + backend), opts)) {
            DifferentialTest.compareFullState(e, ref, cfg);
        }
    }
}
