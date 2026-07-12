package io.chronodim.testkit;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.InputRow;
import io.chronodim.api.QualityGate;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Independent, obviously-correct reference SCD2 implementation for differential
 * testing (R-TEST-1). Deliberately different mechanics from the engine: chains
 * live in TreeMaps keyed by validFrom, change detection compares actual values
 * (not hashes), and the bitemporal log is a plain append-only list per key.
 *
 * <p>Belief time is modeled as the <b>batch ordinal</b>: batch {@code b} creates
 * records with beliefAt = b. Bitemporal queries take a batch ordinal.
 */
public final class ReferenceScd2 {

    public static final long OPEN = Long.MAX_VALUE;

    /** One stored record (a belief about a validity interval). */
    public record Rec(long validFrom, long validTo, String op, int beliefAt, Map<String, Object> row) {}

    private final TableConfig cfg;
    private final Map<String, List<Rec>> log = new HashMap<>();
    private final Map<String, TreeMap<Long, Rec>> chains = new HashMap<>();
    private int batches;

    public ReferenceScd2(TableConfig cfg) {
        this.cfg = cfg;
    }

    public int batchCount() {
        return batches;
    }

    /** Applies one batch; returns per-batch counters {inserts,updates,noops,deletes,lateSplits,rejects,quarantined}. */
    public Map<String, Long> applyBatch(List<InputRow> rows, long loadTimeMicros) {
        return applyBatch(rows, loadTimeMicros, false);
    }

    /** With {@code fullSnapshot}: active keys absent from the batch are soft-deleted at load time. */
    public Map<String, Long> applyBatch(List<InputRow> rows, long loadTimeMicros, boolean fullSnapshot) {
        int belief = batches++;
        Map<String, Long> counters = new LinkedHashMap<>();
        for (String k : List.of("inserts", "updates", "no_ops", "deletes", "late_splits", "rejects", "quarantined")) {
            counters.put(k, 0L);
        }
        // coerce + gate + group (mirrors the public contract, not the engine internals)
        Map<String, List<Pending>> groups = new LinkedHashMap<>();
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        for (InputRow in : rows) {
            Pending p;
            try {
                p = coerce(in, loadTimeMicros);
            } catch (ValidationException e) {
                bump(counters, failureCounter());
                continue;
            }
            seenKeys.add(p.key);
            String gate = gateFail(p.row);
            if (gate != null) {
                bump(counters, failureCounter());
                continue;
            }
            groups.computeIfAbsent(p.key, k -> new ArrayList<>()).add(p);
        }
        if (fullSnapshot) {
            for (String key : new ArrayList<>(chains.keySet())) {
                if (seenKeys.contains(key) || current(key) == null) continue;
                groups.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new Pending(key, loadTimeMicros, true, new LinkedHashMap<>()));
            }
        }

        for (Map.Entry<String, List<Pending>> g : groups.entrySet()) {
            List<Pending> list = g.getValue();
            list.sort(Comparator.comparingLong(p -> p.validFrom)); // stable
            List<Pending> dedup = new ArrayList<>();
            for (Pending p : list) {
                if (!dedup.isEmpty() && dedup.get(dedup.size() - 1).validFrom == p.validFrom) {
                    if (cfg.duplicatePolicy() == TableConfig.DuplicatePolicy.LAST_WINS) {
                        dedup.set(dedup.size() - 1, p);
                    } else {
                        bump(counters, "rejects");
                    }
                } else {
                    dedup.add(p);
                }
            }
            for (Pending p : dedup) {
                place(g.getKey(), p, belief, counters);
            }
        }
        return counters;
    }

    private String failureCounter() {
        return cfg.batchFailurePolicy() == TableConfig.BatchFailurePolicy.QUARANTINE ? "quarantined" : "rejects";
    }

    private void place(String key, Pending p, int belief, Map<String, Long> counters) {
        TreeMap<Long, Rec> chain = chains.computeIfAbsent(key, k -> new TreeMap<>());
        long vf = p.validFrom;

        boolean late = !chain.isEmpty() && vf < chain.lastKey();
        if (late && cfg.lateArrivalPolicy() != TableConfig.LateArrivalPolicy.SPLIT) {
            bump(counters, cfg.lateArrivalPolicy() == TableConfig.LateArrivalPolicy.REJECT ? "rejects" : "quarantined");
            return;
        }
        if (p.delete && cfg.deleteMode() == TableConfig.DeleteMode.IGNORE) {
            bump(counters, "no_ops");
            return;
        }

        Rec exact = chain.get(vf);
        Map.Entry<Long, Rec> pe = chain.lowerEntry(vf);
        Map.Entry<Long, Rec> ne = chain.higherEntry(vf);

        if (p.delete) {
            if (exact != null) {
                if (exact.op.equals("DELETE")) {
                    bump(counters, "no_ops");
                    return;
                }
                put(key, chain, new Rec(vf, exact.validTo, "DELETE", belief, exact.row));
                bump(counters, "deletes");
                if (ne != null) bump(counters, "late_splits");
                return;
            }
            if (pe == null || pe.getValue().op.equals("DELETE") || pe.getValue().validTo <= vf) {
                bump(counters, "no_ops");
                return;
            }
            Rec prev = pe.getValue();
            put(key, chain, new Rec(prev.validFrom, vf, prev.op, belief, prev.row));
            put(key, chain, new Rec(vf, prev.validTo, "DELETE", belief, prev.row));
            bump(counters, "deletes");
            if (ne != null) bump(counters, "late_splits");
            return;
        }

        if (exact != null) {
            if (!exact.op.equals("DELETE") && trackedEquals(exact.row, p.row)) {
                bump(counters, "no_ops");
                return;
            }
            String op = (pe == null || pe.getValue().op.equals("DELETE")) ? "INSERT" : "UPDATE";
            put(key, chain, new Rec(vf, exact.validTo, op, belief, p.row));
            bump(counters, "updates");
            if (late) bump(counters, "late_splits");
            return;
        }

        if (pe != null && pe.getValue().validTo > vf && !pe.getValue().op.equals("DELETE")
                && trackedEquals(pe.getValue().row, p.row)) {
            bump(counters, "no_ops");
            return;
        }

        long vt = ne != null ? ne.getKey() : OPEN;
        String op = (pe == null || pe.getValue().op.equals("DELETE")) ? "INSERT" : "UPDATE";
        if (pe != null && pe.getValue().validTo > vf) {
            Rec prev = pe.getValue();
            put(key, chain, new Rec(prev.validFrom, vf, prev.op, belief, prev.row));
        }
        put(key, chain, new Rec(vf, vt, op, belief, p.row));
        bump(counters, op.equals("INSERT") ? "inserts" : "updates");
        if (late) bump(counters, "late_splits");
    }

    private void put(String key, TreeMap<Long, Rec> chain, Rec rec) {
        chain.put(rec.validFrom(), rec);
        List<Rec> recs = log.computeIfAbsent(key, k -> new ArrayList<>());
        // One belief per (validFrom, batch): rewriting the same instant within one
        // batch coalesces, matching the engine's one-record-per-(vf, txn) contract.
        for (int i = recs.size() - 1; i >= 0; i--) {
            Rec r = recs.get(i);
            if (r.beliefAt() != rec.beliefAt()) break;
            if (r.validFrom() == rec.validFrom()) {
                recs.set(i, rec);
                return;
            }
        }
        recs.add(rec);
    }

    // ---- queries -------------------------------------------------------------------

    public java.util.Set<String> keys() {
        // Keys that produced at least one stored record (rejected/no-op-only keys
        // never materialize in the engine either).
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (Map.Entry<String, List<Rec>> e : log.entrySet()) {
            if (!e.getValue().isEmpty()) out.add(e.getKey());
        }
        return out;
    }

    /** Latest-knowledge current version, or null. */
    public Rec current(String key) {
        TreeMap<Long, Rec> chain = chains.get(key);
        if (chain == null || chain.isEmpty()) return null;
        Rec last = chain.lastEntry().getValue();
        return !last.op.equals("DELETE") && last.validTo == OPEN ? last : null;
    }

    /** Latest-knowledge as-of version, or null. */
    public Rec asOf(String key, long validTime) {
        TreeMap<Long, Rec> chain = chains.get(key);
        if (chain == null) return null;
        Map.Entry<Long, Rec> e = chain.floorEntry(validTime);
        if (e == null) return null;
        Rec r = e.getValue();
        if (r.op.equals("DELETE") || validTime >= r.validTo) return null;
        return r;
    }

    /** Bitemporal as-of: what was believed after batch {@code beliefAt} completed. */
    public Rec asOf(String key, long validTime, int beliefAt) {
        List<Rec> recs = log.getOrDefault(key, List.of());
        TreeMap<Long, Rec> asBelieved = new TreeMap<>();
        for (Rec r : recs) {
            if (r.beliefAt() <= beliefAt) asBelieved.put(r.validFrom(), r); // later records overwrite: newest belief wins
        }
        Map.Entry<Long, Rec> e = asBelieved.floorEntry(validTime);
        if (e == null) return null;
        Rec r = e.getValue();
        if (r.op.equals("DELETE") || validTime >= r.validTo) return null;
        return r;
    }

    /** Full record log, ordered like engine history: validFrom DESC, then recency DESC. */
    public List<Rec> history(String key) {
        List<Rec> recs = new ArrayList<>(log.getOrDefault(key, List.of()));
        List<Rec> indexed = recs; // insertion order = belief order
        List<int[]> order = new ArrayList<>();
        for (int i = 0; i < indexed.size(); i++) order.add(new int[]{i});
        order.sort((a, b) -> {
            Rec ra = indexed.get(a[0]), rb = indexed.get(b[0]);
            int c = Long.compare(rb.validFrom(), ra.validFrom());
            if (c != 0) return c;
            return Integer.compare(b[0], a[0]); // later write first
        });
        List<Rec> out = new ArrayList<>(indexed.size());
        for (int[] i : order) out.add(indexed.get(i[0]));
        return out;
    }

    /** All current rows keyed by business key string. */
    public Map<String, Rec> currentSet() {
        Map<String, Rec> out = new LinkedHashMap<>();
        for (String k : chains.keySet()) {
            Rec r = current(k);
            if (r != null) out.put(k, r);
        }
        return out;
    }

    public Map<String, Rec> asOfSet(long validTime) {
        Map<String, Rec> out = new LinkedHashMap<>();
        for (String k : chains.keySet()) {
            Rec r = asOf(k, validTime);
            if (r != null) out.put(k, r);
        }
        return out;
    }

    // ---- helpers ---------------------------------------------------------------------

    public String keyOf(Map<String, Object> coercedRow) {
        StringBuilder sb = new StringBuilder();
        for (String k : cfg.businessKey()) {
            sb.append(render(cfg.currentSchema().column(k), coercedRow.get(k))).append('');
        }
        return sb.toString();
    }

    private record Pending(String key, long validFrom, boolean delete, Map<String, Object> row) {}

    private Pending coerce(InputRow in, long loadTime) {
        TableSchema schema = cfg.currentSchema();
        Map<String, Object> row = new LinkedHashMap<>();
        for (Column c : schema.columns()) {
            row.put(c.name(), c.type().coerce(in.values().get(c.name())));
        }
        for (String k : cfg.businessKey()) {
            if (row.get(k) == null) throw new ValidationException("null business key");
        }
        long vf;
        if (in.validFromMicrosOverride() != null) {
            vf = in.validFromMicrosOverride();
        } else if (cfg.validTimeMode() == TableConfig.ValidTimeMode.SOURCE_COLUMN) {
            Object v = row.get(cfg.validTimeColumn());
            if (v == null) throw new ValidationException("null valid time");
            Column vc = schema.column(cfg.validTimeColumn());
            vf = vc.kind() == ColumnType.DATE ? (Integer) v * 86_400_000_000L : (Long) v;
        } else {
            vf = loadTime;
        }
        return new Pending(keyOf(row), vf, in.delete(), row);
    }

    private String gateFail(Map<String, Object> row) {
        for (QualityGate g : cfg.qualityGates()) {
            String f = g.evaluate(row.get(g.column()));
            if (f != null) return f;
        }
        return null;
    }

    private boolean trackedEquals(Map<String, Object> a, Map<String, Object> b) {
        for (String c : cfg.effectiveTrackedColumns()) {
            Column col = cfg.currentSchema().column(c);
            if (!Objects.equals(render(col, a.get(c)), render(col, b.get(c)))) return false;
        }
        return true;
    }

    /** Canonical comparable rendering (handles byte[]/BigDecimal). */
    public static Object render(Column col, Object v) {
        return v == null ? null : col.type().render(v);
    }

    private static void bump(Map<String, Long> counters, String k) {
        counters.merge(k, 1L, Long::sum);
    }
}
