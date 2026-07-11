package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cornerstone (R-TEST-1): random change streams — late arrivals, same-batch
 * duplicates, deletes, re-inserts — applied to the engine and to the independent
 * reference implementation; full state compared after every batch on both
 * temporal axes, for both storage backends.
 */
class DifferentialTest {

    @TempDir
    Path dir;

    @ParameterizedTest(name = "backend={0} seed={1}")
    @CsvSource({
            "ROCKSDB, 1", "ROCKSDB, 2", "ROCKSDB, 3",
            "LSM, 1", "LSM, 2", "LSM, 3",
    })
    void engineMatchesReference(String backend, long seed) {
        TableConfig cfg = DataGen.testTable("diff");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(seed, DataGen.StreamOptions.defaults());

        EngineOptions opts = EngineOptions.builder()
                .storageBackend(EngineOptions.StorageBackend.valueOf(backend))
                .groupCommitWindowMicros(0)
                .memtableFlushBytes(64 << 10) // tiny: force flush/compaction during the test
                .maxSegmentsBeforeCompaction(3)
                .build();

        List<Long> batchTxTimes = new ArrayList<>();
        try (Engine e = ChronoDim.open(dir.resolve(backend + "-" + seed), opts)) {
            e.createTable(cfg);
            int batches = 25;
            for (int b = 0; b < batches; b++) {
                List<InputRow> rows = gen.nextBatch(40);
                AuditManifest m = e.apply(ApplyBatch.single("batch-" + b, "diff", rows));
                ref.applyBatch(rows, m.startedAtMicros());
                batchTxTimes.add(m.startedAtMicros());

                compareFullState(e, ref, cfg);
            }
            compareBitemporal(e, ref, cfg, batchTxTimes);
        }
    }

    static void compareFullState(Engine e, ReferenceScd2 ref, TableConfig cfg) {
        // Current set via streaming scan
        Map<String, ReferenceScd2.Rec> expected = ref.currentSet();
        Map<String, Map<String, Object>> actual = new LinkedHashMap<>();
        try (VersionCursor c = e.scanCurrent(cfg.table())) {
            while (c.hasNext()) {
                Version v = c.next();
                actual.put(String.valueOf(v.row().get("customer_id")), DataGen.rendered(cfg.currentSchema(), v.row()));
            }
        }
        assertEquals(expected.size(), actual.size(), "current-set size");
        for (Map.Entry<String, ReferenceScd2.Rec> en : expected.entrySet()) {
            String key = String.valueOf(en.getValue().row().get("customer_id"));
            assertEquals(renderRef(cfg, en.getValue().row()), actual.get(key), "current row for " + key);
        }

        // Per-key history on both axes
        for (String refKey : ref.keys()) {
            ReferenceScd2.Rec any = ref.history(refKey).get(0);
            Map<String, Object> bk = Map.of("customer_id", any.row().get("customer_id"));
            List<ReferenceScd2.Rec> expectedHist = ref.history(refKey);
            List<Version> actualHist = e.getHistory(cfg.table(), bk);
            assertEquals(expectedHist.size(), actualHist.size(),
                    "history length for " + refKey + "\nref=" + expectedHist + "\nact=" + describe(actualHist));
            for (int i = 0; i < expectedHist.size(); i++) {
                ReferenceScd2.Rec r = expectedHist.get(i);
                Version v = actualHist.get(i);
                assertEquals(r.validFrom(), v.validFrom(), "validFrom[" + i + "] for " + refKey);
                assertEquals(r.validTo(), v.validTo(), "validTo[" + i + "] for " + refKey
                        + "\nref=" + expectedHist + "\nact=" + describe(actualHist));
                assertEquals(r.op(), v.op().name(), "op[" + i + "] for " + refKey);
                assertEquals(renderRef(cfg, r.row()), DataGen.rendered(cfg.currentSchema(), v.row()),
                        "row[" + i + "] for " + refKey);
            }
        }
    }

    static void compareBitemporal(Engine e, ReferenceScd2 ref, TableConfig cfg, List<Long> batchTxTimes) {
        Random rnd = new Random(99);
        long start = io.chronodim.api.ColumnType.parseTimestampMicros("2023-11-01T00:00:00Z");
        long end = io.chronodim.api.ColumnType.parseTimestampMicros("2025-06-01T00:00:00Z");
        for (String refKey : ref.keys()) {
            Object customerId = ref.history(refKey).get(0).row().get("customer_id");
            Map<String, Object> bk = Map.of("customer_id", customerId);
            for (int probe = 0; probe < 12; probe++) {
                long validTime = start + (long) (rnd.nextDouble() * (end - start));
                int batch = rnd.nextInt(batchTxTimes.size());
                ReferenceScd2.Rec expected = ref.asOf(refKey, validTime, batch);
                var actual = e.getAsOf(cfg.table(), bk, validTime, batchTxTimes.get(batch));
                if (expected == null) {
                    assertTrue(actual.isEmpty(), "expected empty bitemporal read for " + refKey
                            + " valid=" + validTime + " batch=" + batch + " got " + actual);
                } else {
                    assertTrue(actual.isPresent(), "expected present bitemporal read for " + refKey
                            + " valid=" + validTime + " batch=" + batch);
                    assertEquals(expected.validFrom(), actual.get().validFrom());
                    assertEquals(renderRef(cfg, expected.row()),
                            DataGen.rendered(cfg.currentSchema(), actual.get().row()));
                }
            }
        }
    }

    private static Map<String, Object> renderRef(TableConfig cfg, Map<String, Object> row) {
        return DataGen.rendered(cfg.currentSchema(), row);
    }

    private static String describe(List<Version> hist) {
        StringBuilder sb = new StringBuilder();
        for (Version v : hist) {
            sb.append("[vf=").append(v.validFrom()).append(" vt=").append(v.validTo())
                    .append(" op=").append(v.op()).append(" tx=").append(v.txTime()).append("]\n");
        }
        return sb.toString();
    }
}
