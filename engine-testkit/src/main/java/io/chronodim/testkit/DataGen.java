package io.chronodim.testkit;

import io.chronodim.api.Column;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.core.catalog.TableConfigIO;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Synthetic data for tests and benchmarks. Change streams cover the hard cases:
 * late arrivals, same-key same-batch collisions, deletes, re-inserts after delete
 * (R-TEST-1).
 */
public final class DataGen {

    /** The standard differential-test table: every column type, source valid time. */
    public static TableConfig testTable(String name) {
        return TableConfigIO.fromYaml("""
                table: %s
                business_key: [customer_id]
                schema:
                  - {name: customer_id, type: string}
                  - {name: name,        type: string}
                  - {name: segment,     type: string}
                  - {name: risk_score,  type: double}
                  - {name: exposure,    type: "decimal(18,4)"}
                  - {name: active,      type: boolean}
                  - {name: onboarded,   type: date}
                  - {name: score_count, type: long}
                  - {name: token,       type: bytes}
                  - {name: updated_at,  type: timestamp}
                  - {name: noise,       type: string}
                tracked_columns: [name, segment, risk_score, exposure, active, onboarded, score_count, token]
                ignored_columns: [noise]
                valid_time:
                  mode: source_column
                  column: updated_at
                late_arrival:
                  policy: split
                deletes:
                  mode: soft
                """.formatted(name));
    }

    /** A simple benchmark table: 12 columns, ~300B/row (§10). */
    public static TableConfig benchTable(String name) {
        StringBuilder cols = new StringBuilder();
        for (int i = 1; i <= 9; i++) {
            cols.append("  - {name: attr_").append(i).append(", type: string}\n");
        }
        return TableConfigIO.fromYaml("""
                table: %s
                business_key: [id]
                schema:
                  - {name: id,         type: string}
                %s  - {name: amount,     type: double}
                  - {name: updated_at, type: timestamp}
                valid_time:
                  mode: source_column
                  column: updated_at
                """.formatted(name, cols));
    }

    public record StreamOptions(int keys, double lateProb, double deleteProb, double dupProb, double noChangeProb) {
        public static StreamOptions defaults() {
            return new StreamOptions(20, 0.15, 0.07, 0.05, 0.2);
        }
    }

    private final Random rnd;
    private final StreamOptions opts;
    private final Map<String, Map<String, Object>> lastValues = new LinkedHashMap<>();
    private long clock = ColumnTypeUtil.micros("2024-01-01T00:00:00Z");

    public DataGen(long seed, StreamOptions opts) {
        this.rnd = new Random(seed);
        this.opts = opts;
    }

    /** One CDC batch for the {@link #testTable} schema. */
    public List<InputRow> nextBatch(int rows) {
        List<InputRow> out = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            String key = "C" + (1 + rnd.nextInt(opts.keys()));
            clock += 1_000_000L * (1 + rnd.nextInt(3600));
            long ts = rnd.nextDouble() < opts.lateProb()
                    ? clock - 86_400_000_000L * (1 + rnd.nextInt(30)) // up to 30 days late
                    : clock;
            boolean delete = rnd.nextDouble() < opts.deleteProb();
            Map<String, Object> v = delete ? deleteRow(key, ts) : upsertRow(key, ts);
            out.add(new InputRow(v, delete));
            if (!delete && rnd.nextDouble() < opts.dupProb()) {
                // same key, same valid_from, different value → duplicate-policy exercise
                Map<String, Object> dup = upsertRow(key, ts);
                out.add(new InputRow(dup, false));
            }
        }
        return out;
    }

    private Map<String, Object> upsertRow(String key, long ts) {
        Map<String, Object> prev = lastValues.get(key);
        Map<String, Object> v = new LinkedHashMap<>();
        boolean noChange = prev != null && rnd.nextDouble() < opts.noChangeProb();
        v.put("customer_id", key);
        if (noChange) {
            v.putAll(prev);
        } else {
            v.put("name", "Name-" + rnd.nextInt(1000));
            v.put("segment", List.of("RETAIL", "SME", "CORP", "PRIVATE").get(rnd.nextInt(4)));
            v.put("risk_score", Math.round(rnd.nextDouble() * 100000.0) / 100.0);
            v.put("exposure", java.math.BigDecimal.valueOf(rnd.nextInt(10_000_000), 4));
            v.put("active", rnd.nextBoolean());
            v.put("onboarded", "20" + (10 + rnd.nextInt(15)) + "-0" + (1 + rnd.nextInt(9)) + "-1" + rnd.nextInt(9));
            v.put("score_count", (long) rnd.nextInt(500));
            byte[] token = new byte[8];
            rnd.nextBytes(token);
            v.put("token", token);
            Map<String, Object> tracked = new LinkedHashMap<>(v);
            tracked.remove("customer_id");
            lastValues.put(key, tracked);
        }
        v.put("updated_at", ts);
        v.put("noise", "noise-" + rnd.nextInt()); // ignored column must never create versions
        return v;
    }

    private Map<String, Object> deleteRow(String key, long ts) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("customer_id", key);
        v.put("updated_at", ts);
        return v;
    }

    /** Benchmark row for {@link #benchTable}. */
    public static Map<String, Object> benchRow(long id, long ts, Random rnd) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", "K" + id);
        for (int i = 1; i <= 9; i++) {
            v.put("attr_" + i, "value-" + i + "-" + rnd.nextInt(1_000_000) + "-padding-padding");
        }
        v.put("amount", rnd.nextDouble() * 1e6);
        v.put("updated_at", ts);
        return v;
    }

    static final class ColumnTypeUtil {
        static long micros(String iso) {
            return io.chronodim.api.ColumnType.parseTimestampMicros(iso);
        }
    }

    /** Renders a coerced engine row for comparison with the reference. */
    public static Map<String, Object> rendered(TableSchema schema, Map<String, Object> row) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Column c : schema.columns()) {
            out.put(c.name(), row.get(c.name()) == null ? null : c.type().render(row.get(c.name())));
        }
        return out;
    }
}
