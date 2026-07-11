package io.chronodim.cli;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ColumnType;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.core.catalog.TableConfigIO;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The in-CLI standard benchmark (§10 scenarios P1/P2/P3, sized by flags).
 * engine-bench runs the full report; this gives an on-box sanity number.
 */
final class BenchCore {
    private BenchCore() {}

    static Map<String, Object> run(Path dataDir, EngineOptions options, long rows, long changes, boolean quiet) {
        TableConfig cfg = TableConfigIO.fromYaml("""
                table: bench
                business_key: [id]
                schema:
                  - {name: id,         type: string}
                  - {name: attr_1,     type: string}
                  - {name: attr_2,     type: string}
                  - {name: attr_3,     type: string}
                  - {name: attr_4,     type: string}
                  - {name: attr_5,     type: string}
                  - {name: attr_6,     type: string}
                  - {name: attr_7,     type: string}
                  - {name: attr_8,     type: string}
                  - {name: attr_9,     type: string}
                  - {name: amount,     type: double}
                  - {name: updated_at, type: timestamp}
                valid_time:
                  mode: source_column
                  column: updated_at
                """);
        Map<String, Object> report = new LinkedHashMap<>();
        long baseTs = ColumnType.parseTimestampMicros("2024-01-01T00:00:00Z");

        try (Engine e = ChronoDim.open(dataDir, options.toBuilder().publishEnabled(false).build())) {
            e.createTable(cfg);

            // P1: bulk backfill
            long t0 = System.nanoTime();
            e.backfill("bench-bulk", "bench", new RowIterator(rows, baseTs));
            double bulkSecs = (System.nanoTime() - t0) / 1e9;
            report.put("bulk_rows", rows);
            report.put("bulk_seconds", round(bulkSecs));
            report.put("bulk_rows_per_sec", Math.round(rows / bulkSecs));

            // P2: CDC apply (70% updates, 20% no-ops, 8% inserts, 2% deletes)
            Random rnd = new Random(1234);
            long applied = 0;
            long t1 = System.nanoTime();
            int batchSize = 10_000;
            int batchNo = 0;
            while (applied < changes) {
                int n = (int) Math.min(batchSize, changes - applied);
                List<InputRow> batch = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    double p = rnd.nextDouble();
                    long id = 1 + (long) (rnd.nextDouble() * rows);
                    long ts = baseTs + 86_400_000_000L + applied + i;
                    if (p < 0.02) {
                        batch.add(InputRow.delete(Map.of("id", "K" + id, "updated_at", ts)));
                    } else if (p < 0.10) {
                        batch.add(InputRow.upsert(row("NEW" + (applied + i), ts, rnd, false)));
                    } else if (p < 0.30) {
                        batch.add(InputRow.upsert(row("K" + id, baseTs + (id % 1000), rnd, true))); // same values → no-op
                    } else {
                        batch.add(InputRow.upsert(row("K" + id, ts, rnd, false)));
                    }
                }
                e.apply(ApplyBatch.single("bench-cdc-" + (batchNo++), "bench", batch));
                applied += n;
            }
            double cdcSecs = (System.nanoTime() - t1) / 1e9;
            report.put("cdc_changes", changes);
            report.put("cdc_seconds", round(cdcSecs));
            report.put("cdc_applies_per_sec", Math.round(changes / cdcSecs));

            // P3: warm point reads
            long[] lat = new long[10_000];
            for (int i = 0; i < lat.length; i++) {
                long id = 1 + (long) (rnd.nextDouble() * rows);
                long s = System.nanoTime();
                e.getCurrent("bench", Map.of("id", "K" + id));
                lat[i] = System.nanoTime() - s;
            }
            Arrays.sort(lat);
            report.put("read_p50_micros", lat[lat.length / 2] / 1_000);
            report.put("read_p99_micros", lat[(int) (lat.length * 0.99)] / 1_000);
            report.put("read_max_micros", lat[lat.length - 1] / 1_000);
        }
        return report;
    }

    private static Map<String, Object> row(String id, long ts, Random rnd, boolean stable) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        for (int i = 1; i <= 9; i++) {
            v.put("attr_" + i, stable ? "stable-" + i + "-" + id : "value-" + i + "-" + rnd.nextInt(1_000_000) + "-padding");
        }
        v.put("amount", stable ? 1.0 : rnd.nextDouble() * 1e6);
        v.put("updated_at", ts);
        return v;
    }

    private static double round(double d) {
        return Math.round(d * 100.0) / 100.0;
    }

    /** Streaming synthetic rows so 50M-row runs never materialize the input. */
    private static final class RowIterator implements Iterable<InputRow> {
        private final long rows;
        private final long baseTs;

        RowIterator(long rows, long baseTs) {
            this.rows = rows;
            this.baseTs = baseTs;
        }

        @Override
        public Iterator<InputRow> iterator() {
            Random rnd = new Random(42);
            return new Iterator<>() {
                long i = 0;

                @Override
                public boolean hasNext() {
                    return i < rows;
                }

                @Override
                public InputRow next() {
                    i++;
                    return InputRow.upsert(row("K" + i, baseTs + (i % 1000), rnd, false));
                }
            };
        }
    }
}
