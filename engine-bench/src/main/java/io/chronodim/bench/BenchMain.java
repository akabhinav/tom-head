package io.chronodim.bench;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ColumnType;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.util.Json;
import io.chronodim.testkit.DataGen;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The standard benchmark workload (§10) producing a machine-readable report
 * (R-TEST-4). Scenarios P1 (bulk), P2 (CDC mix), P3 (warm point reads),
 * P4 (cold reads after reopen), P5 (full scan).
 *
 * <pre>
 * mvn -pl engine-bench -am package -q -DskipTests
 * java -cp engine-bench/target/classes:... io.chronodim.bench.BenchMain \
 *      --data-dir /nvme/benchdb --rows 50000000 --changes 2000000 --report report.json
 * </pre>
 */
public final class BenchMain {

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        Path dataDir = Path.of(a.getOrDefault("data-dir", "./chronodim-bench"));
        long rows = Long.parseLong(a.getOrDefault("rows", "1000000"));
        long changes = Long.parseLong(a.getOrDefault("changes", "200000"));
        String backend = a.getOrDefault("storage", "rocksdb").toUpperCase(java.util.Locale.ROOT);
        Path reportFile = Path.of(a.getOrDefault("report", "chronodim-bench-report.json"));

        if (Files.exists(dataDir) && Files.list(dataDir).findAny().isPresent()) {
            System.err.println("error: --data-dir must be empty");
            System.exit(2);
        }

        EngineOptions opts = EngineOptions.builder()
                .storageBackend(EngineOptions.StorageBackend.valueOf(backend))
                .publishEnabled(false)
                .build();

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("engine", "chronodim-1.0.0");
        report.put("storage_backend", backend);
        report.put("rows", rows);
        report.put("changes", changes);
        report.put("cores", (long) Runtime.getRuntime().availableProcessors());
        report.put("heap_max_mb", Runtime.getRuntime().maxMemory() >> 20);

        long baseTs = ColumnType.parseTimestampMicros("2024-01-01T00:00:00Z");

        // ---- P1: bulk backfill ----
        try (Engine e = ChronoDim.open(dataDir, opts)) {
            e.createTable(DataGen.benchTable("bench"));
            long t0 = System.nanoTime();
            e.backfill("p1", "bench", bulkRows(rows, baseTs));
            double secs = (System.nanoTime() - t0) / 1e9;
            put(report, "P1_bulk", Map.of("seconds", secs, "rows_per_sec", (long) (rows / secs),
                    "pass_5min_target", secs <= 300));

            // ---- P2: CDC mix (70/20/8/2) ----
            Random rnd = new Random(1);
            long t1 = System.nanoTime();
            long applied = 0;
            int batchNo = 0;
            while (applied < changes) {
                int n = (int) Math.min(20_000, changes - applied);
                List<InputRow> batch = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    double p = rnd.nextDouble();
                    long id = 1 + (long) (rnd.nextDouble() * rows);
                    long ts = baseTs + 86_400_000_000L + applied + i;
                    if (p < 0.02) {
                        batch.add(InputRow.delete(Map.of("id", "K" + id, "updated_at", ts)));
                    } else if (p < 0.10) {
                        batch.add(InputRow.upsert(DataGen.benchRow(rows + applied + i, ts, rnd)));
                    } else if (p < 0.30) {
                        batch.add(InputRow.upsert(sameRow(id, baseTs, ts)));
                    } else {
                        batch.add(InputRow.upsert(DataGen.benchRow(id, ts, rnd)));
                    }
                }
                e.apply(ApplyBatch.single("p2-" + (batchNo++), "bench", batch));
                applied += n;
            }
            double cdcSecs = (System.nanoTime() - t1) / 1e9;
            put(report, "P2_cdc", Map.of("seconds", cdcSecs, "applies_per_sec", (long) (changes / cdcSecs),
                    "pass_100k_target", changes / cdcSecs >= 100_000));

            // ---- P3: warm point reads ----
            put(report, "P3_warm_reads", latencies(e, rows, 20_000, rnd));

            // ---- P5: full current scan ----
            long t2 = System.nanoTime();
            long scanned = 0;
            try (VersionCursor c = e.scanCurrent("bench")) {
                while (c.hasNext()) {
                    c.next();
                    scanned++;
                }
            }
            double scanSecs = (System.nanoTime() - t2) / 1e9;
            put(report, "P5_scan", Map.of("rows", scanned, "seconds", scanSecs,
                    "rows_per_sec", (long) (scanned / scanSecs), "pass_2m_target", scanned / scanSecs >= 2_000_000));
        }

        // ---- P4: cold reads after restart ----
        try (Engine e = ChronoDim.open(dataDir, opts)) {
            put(report, "P4_cold_reads", latencies(e, rows, 5_000, new Random(2)));
        }

        String json = Json.writePretty(report);
        Files.writeString(reportFile, json);
        System.out.println(json);
    }

    private static Map<String, Object> latencies(Engine e, long rows, int probes, Random rnd) {
        long[] lat = new long[probes];
        for (int i = 0; i < probes; i++) {
            long id = 1 + (long) (rnd.nextDouble() * rows);
            long s = System.nanoTime();
            e.getCurrent("bench", Map.of("id", "K" + id));
            lat[i] = System.nanoTime() - s;
        }
        Arrays.sort(lat);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("p50_micros", lat[probes / 2] / 1_000);
        m.put("p99_micros", lat[(int) (probes * 0.99)] / 1_000);
        m.put("max_micros", lat[probes - 1] / 1_000);
        return m;
    }

    private static Map<String, Object> sameRow(long id, long baseTs, long ts) {
        Random stable = new Random(id); // same seed → same attrs → hash-equal no-op
        Map<String, Object> v = DataGen.benchRow(id, baseTs + (id % 1000), stable);
        v.put("updated_at", ts);
        return v;
    }

    private static Iterable<InputRow> bulkRows(long rows, long baseTs) {
        return () -> new Iterator<>() {
            long i = 0;

            @Override
            public boolean hasNext() {
                return i < rows;
            }

            @Override
            public InputRow next() {
                i++;
                Random stable = new Random(i);
                return InputRow.upsert(DataGen.benchRow(i, baseTs + (i % 1000), stable));
            }
        };
    }

    private static void put(Map<String, Object> report, String key, Map<String, Object> value) {
        report.put(key, new LinkedHashMap<>(value));
        System.err.println("[bench] " + key + " " + Json.write(value));
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String k = args[i].substring(2);
                String v = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
                m.put(k, v);
            }
        }
        return m;
    }

    private BenchMain() {}
}
