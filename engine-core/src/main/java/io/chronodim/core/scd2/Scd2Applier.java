package io.chronodim.core.scd2;

import io.chronodim.api.AuditManifest;
import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.InputRow;
import io.chronodim.api.Op;
import io.chronodim.api.QualityGate;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;
import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.StorageEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The SCD2 apply algorithm (R-APPLY-2/5/6). Stateless: reads the current chains
 * from storage, computes the mutation set for one transaction into an
 * {@link AtomicBatch}, and reports per-table counters.
 *
 * <p>Chain model: for each entity, storage holds every record ever written,
 * keyed by (validFrom DESC, txTime DESC). "Latest knowledge" = the newest-txTime
 * record of each validFrom group. Superseding a version writes a new record with
 * the same validFrom and the current txTime — never destroying prior belief
 * (bitemporal rule §5.3).
 */
public final class Scd2Applier {

    /** Mutable per-table counters, copied into the audit manifest. */
    public static final class Counters {
        public long rowsIn, inserts, updates, noOps, deletes, lateSplits, rejects, quarantined;

        public AuditManifest.TableStats toStats(String table, String configHash, int schemaVersion) {
            return new AuditManifest.TableStats(table, configHash, schemaVersion,
                    rowsIn, inserts, updates, noOps, deletes, lateSplits, rejects, quarantined);
        }
    }

    /** Callback for rows diverted to quarantine. */
    public interface QuarantineSink {
        void quarantine(TableRuntime rt, Map<String, Object> rawValues, boolean delete, String reason);
    }

    /** Callback for row errors (bounded by the engine). */
    public interface ErrorSink {
        void error(String table, long rowIndex, String reason);
    }

    private final StorageEngine storage;

    public Scd2Applier(StorageEngine storage) {
        this.storage = storage;
    }

    /**
     * Computes the mutations for one table batch. Throws {@link ValidationException}
     * when the table's policy is FAIL_BATCH and any row fails.
     */
    public Counters applyTable(TableRuntime rt, List<InputRow> rows, long txTime, long txnId,
                               AtomicBatch out, ErrorSink errors, QuarantineSink quarantine) {
        TableConfig cfg = rt.config();
        TableSchema schema = cfg.currentSchema();
        List<Column> bkCols = rt.businessKeyColumns();
        List<String> tracked = cfg.effectiveTrackedColumns();
        Counters c = new Counters();

        // Phase 1: coerce + validate + group by business key (preserving input order).
        Map<BytesKey, List<PendingRow>> groups = new LinkedHashMap<>();
        long rowIndex = -1;
        for (InputRow in : rows) {
            rowIndex++;
            c.rowsIn++;
            try {
                PendingRow pr = coerce(cfg, schema, bkCols, tracked, in, txTime);
                String gateFailure = gateFailure(cfg, pr.values);
                if (gateFailure != null) {
                    handleRowFailure(cfg, rt, in, c, errors, quarantine, rowIndex, gateFailure);
                    continue;
                }
                groups.computeIfAbsent(new BytesKey(pr.bkBytes), k -> new ArrayList<>()).add(pr);
            } catch (ValidationException e) {
                handleRowFailure(cfg, rt, in, c, errors, quarantine, rowIndex, e.getMessage());
            }
        }

        // Phase 2: per entity — resolve identity, load chain, place rows, emit records.
        Map<Long, java.util.Set<Integer>> claimedInBatch = new java.util.HashMap<>();
        for (Map.Entry<BytesKey, List<PendingRow>> e : groups.entrySet()) {
            byte[] bkBytes = e.getKey().bytes;
            List<PendingRow> pending = dedupe(cfg, rt, e.getValue(), c, errors, quarantine);
            if (pending.isEmpty()) continue;

            EntityRef ref = resolveEntity(rt, bkBytes, out, claimedInBatch);
            List<WorkingVersion> chain = ref.isNew ? new ArrayList<>() : loadChain(rt, ref);

            for (PendingRow pr : pending) {
                placeRow(cfg, rt, chain, pr, c, errors, quarantine);
            }

            emit(rt, ref, bkBytes, chain, schema, txTime, txnId, out);
        }
        return c;
    }

    // ---- phase 1 helpers -------------------------------------------------------

    /** Public coercion result shared with the bulk-backfill path. */
    public record Coerced(Map<String, Object> values, long validFrom, byte[] bkBytes, long attrHash, boolean delete) {}

    /** Coerces + validates one input row against the table's current schema. */
    public static Coerced coerceRow(TableRuntime rt, InputRow in, long defaultValidTime) {
        TableConfig cfg = rt.config();
        PendingRow pr = coerceInternal(cfg, cfg.currentSchema(), rt.businessKeyColumns(),
                cfg.effectiveTrackedColumns(), in, defaultValidTime);
        return new Coerced(pr.values, pr.validFrom, pr.bkBytes, pr.attrHash, pr.delete);
    }

    /** Returns null when all quality gates pass, else the first failure reason. */
    public static String gateFailure(TableConfig cfg, Map<String, Object> values) {
        for (QualityGate g : cfg.qualityGates()) {
            String fail = g.evaluate(values.get(g.column()));
            if (fail != null) return fail;
        }
        return null;
    }

    private PendingRow coerce(TableConfig cfg, TableSchema schema, List<Column> bkCols,
                              List<String> tracked, InputRow in, long txTime) {
        return coerceInternal(cfg, schema, bkCols, tracked, in, txTime);
    }

    private static PendingRow coerceInternal(TableConfig cfg, TableSchema schema, List<Column> bkCols,
                              List<String> tracked, InputRow in, long txTime) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Column col : schema.columns()) {
            Object raw = in.values().get(col.name());
            values.put(col.name(), col.type().coerce(raw, col.scale()));
        }
        Object[] bkValues = new Object[bkCols.size()];
        for (int i = 0; i < bkCols.size(); i++) {
            Object v = values.get(bkCols.get(i).name());
            if (v == null) {
                throw new ValidationException("business key column '" + bkCols.get(i).name() + "' is null or missing");
            }
            bkValues[i] = v;
        }
        long validFrom;
        if (cfg.validTimeMode() == TableConfig.ValidTimeMode.SOURCE_COLUMN) {
            Object v = values.get(cfg.validTimeColumn());
            if (v == null) throw new ValidationException("valid_time column '" + cfg.validTimeColumn() + "' is null");
            Column vc = schema.column(cfg.validTimeColumn());
            validFrom = vc.type() == ColumnType.DATE ? (Integer) v * 86_400_000_000L : (Long) v;
        } else {
            validFrom = txTime;
        }
        byte[] bkBytes = Codecs.encodeBusinessKey(bkCols, bkValues);
        long attrHash = Codecs.attrHash(schema, tracked, values);
        return new PendingRow(values, in.values(), in.delete(), validFrom, bkBytes, attrHash);
    }

    private void handleRowFailure(TableConfig cfg, TableRuntime rt, InputRow in, Counters c,
                                  ErrorSink errors, QuarantineSink quarantine, long rowIndex, String reason) {
        switch (cfg.batchFailurePolicy()) {
            case FAIL_BATCH -> throw new ValidationException(
                    "table '" + cfg.table() + "' row " + rowIndex + ": " + reason + " (policy=fail_batch)");
            case SKIP_ROWS -> {
                c.rejects++;
                errors.error(cfg.table(), rowIndex, reason);
            }
            case QUARANTINE -> {
                c.quarantined++;
                errors.error(cfg.table(), rowIndex, reason + " (quarantined)");
                quarantine.quarantine(rt, in.values(), in.delete(), reason);
            }
        }
    }

    /** Sorts by validFrom (stable) and resolves same-instant duplicates per policy. */
    private List<PendingRow> dedupe(TableConfig cfg, TableRuntime rt, List<PendingRow> rows, Counters c,
                                    ErrorSink errors, QuarantineSink quarantine) {
        rows.sort((a, b) -> Long.compare(a.validFrom, b.validFrom)); // List.sort is stable
        List<PendingRow> out = new ArrayList<>(rows.size());
        for (PendingRow r : rows) {
            PendingRow last = out.isEmpty() ? null : out.get(out.size() - 1);
            if (last != null && last.validFrom == r.validFrom) {
                if (cfg.duplicatePolicy() == TableConfig.DuplicatePolicy.LAST_WINS) {
                    out.set(out.size() - 1, r);
                } else {
                    c.rejects++;
                    errors.error(cfg.table(), -1, "duplicate valid_from " + r.validFrom
                            + " for the same business key within one batch (policy=reject)");
                }
            } else {
                out.add(r);
            }
        }
        return out;
    }

    // ---- entity identity ----------------------------------------------------------

    private record EntityRef(long keyHash, int disambig, boolean isNew) {}

    private EntityRef resolveEntity(TableRuntime rt, byte[] bkBytes, AtomicBatch out,
                                    Map<Long, java.util.Set<Integer>> claimedInBatch) {
        byte[] mapped = storage.get(Codecs.keymapKey(rt.tableId(), bkBytes));
        if (mapped != null) {
            Codecs.KeyRef ref = Codecs.decodeKeymapValue(mapped);
            return new EntityRef(ref.keyHash(), ref.disambig(), false);
        }
        long hash = Codecs.businessKeyHash(bkBytes);
        java.util.Set<Integer> claimedNow = claimedInBatch.computeIfAbsent(hash, h -> new java.util.HashSet<>());
        int disambig = 0;
        while (true) {
            if (!claimedNow.contains(disambig)) {
                byte[] claimed = storage.get(Codecs.hashregKey(rt.tableId(), hash, disambig));
                if (claimed == null || Arrays.equals(claimed, bkBytes)) break; // free, or self-heal
            }
            disambig++;
            if (disambig > 0xFFFF) throw new ValidationException("hash collision chain exhausted (impossible)");
        }
        claimedNow.add(disambig);
        out.put(Codecs.keymapKey(rt.tableId(), bkBytes), Codecs.keymapValue(hash, disambig));
        out.put(Codecs.hashregKey(rt.tableId(), hash, disambig), bkBytes);
        return new EntityRef(hash, disambig, true);
    }

    // ---- chain load / place / emit ---------------------------------------------------

    /**
     * Working copy of one version. {@code storedValue} is the raw record for
     * versions loaded from storage; new/superseding versions carry a payload map.
     */
    private static final class WorkingVersion {
        long validFrom;
        long validTo;
        byte op;
        long attrHash;
        int schemaVersion;
        byte[] storedValue;          // null for brand-new versions
        Map<String, Object> row;     // set for new versions (encoded at current schema)
        boolean dirty;               // validTo changed → supersede with header patch
        boolean isNew;               // brand-new record (new payload)

        static WorkingVersion fromStored(Codecs.DataKey key, byte[] value, Codecs.DecodedValue header) {
            WorkingVersion w = new WorkingVersion();
            w.validFrom = key.validFrom();
            w.validTo = header.validTo();
            w.op = header.op();
            w.attrHash = header.attrHash();
            w.schemaVersion = header.schemaVersion();
            w.storedValue = value;
            return w;
        }
    }

    /** Latest-knowledge chain, ascending validFrom. */
    private List<WorkingVersion> loadChain(TableRuntime rt, EntityRef ref) {
        List<WorkingVersion> chain = new ArrayList<>();
        byte[] prefix = Codecs.entityPrefix(rt.tableId(), ref.keyHash(), ref.disambig());
        long lastVf = Long.MIN_VALUE;
        boolean first = true;
        try (CloseableKvIterator it = storage.prefixScan(prefix)) {
            while (it.hasNext()) {
                KV kv = it.next();
                Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                if (!first && k.validFrom() == lastVf) continue; // superseded belief
                first = false;
                lastVf = k.validFrom();
                chain.add(WorkingVersion.fromStored(k, kv.value(), Codecs.decodeHeader(kv.value())));
            }
        }
        java.util.Collections.reverse(chain); // scan order is validFrom DESC
        return chain;
    }

    private void placeRow(TableConfig cfg, TableRuntime rt, List<WorkingVersion> chain, PendingRow pr,
                          Counters c, ErrorSink errors, QuarantineSink quarantine) {
        long vf = pr.validFrom;
        WorkingVersion latest = chain.isEmpty() ? null : chain.get(chain.size() - 1);

        // Late arrival policy applies to any change strictly before the latest validFrom.
        boolean late = latest != null && vf < latest.validFrom;
        if (late && cfg.lateArrivalPolicy() != TableConfig.LateArrivalPolicy.SPLIT) {
            if (cfg.lateArrivalPolicy() == TableConfig.LateArrivalPolicy.REJECT) {
                c.rejects++;
                errors.error(cfg.table(), -1, "late-arriving valid_from " + ColumnType.formatTimestampMicros(vf)
                        + " < latest " + ColumnType.formatTimestampMicros(latest.validFrom) + " (policy=reject)");
            } else {
                c.quarantined++;
                quarantine.quarantine(rt, pr.rawValues, pr.delete, "late-arriving valid_from (policy=quarantine)");
            }
            return;
        }

        if (pr.delete && cfg.deleteMode() == TableConfig.DeleteMode.IGNORE) {
            c.noOps++;
            return;
        }

        int exact = indexOfExact(chain, vf);
        int pIdx = indexOfFloorBelow(chain, vf);
        WorkingVersion p = pIdx >= 0 ? chain.get(pIdx) : null;
        WorkingVersion n = nextAbove(chain, vf);

        if (pr.delete) {
            if (exact >= 0) {
                WorkingVersion ex = chain.get(exact);
                if (ex.op == Op.DELETE.code) {
                    c.noOps++;
                    return;
                }
                WorkingVersion tomb = tombstoneOf(rt, ex, vf, ex.validTo);
                chain.set(exact, tomb);
                c.deletes++;
                if (n != null) c.lateSplits++;
                return;
            }
            if (p == null || p.op == Op.DELETE.code || p.validTo <= vf) {
                c.noOps++; // nothing alive at that instant
                return;
            }
            WorkingVersion tomb = tombstoneOf(rt, p, vf, p.validTo);
            p.validTo = vf;
            markDirty(p);
            insertSorted(chain, tomb);
            c.deletes++;
            if (n != null) c.lateSplits++;
            return;
        }

        // Upsert.
        if (exact >= 0) {
            WorkingVersion ex = chain.get(exact);
            if (ex.op != Op.DELETE.code && ex.attrHash == pr.attrHash) {
                c.noOps++;
                return;
            }
            WorkingVersion nv = newVersion(pr, ex.validTo, ex.op == Op.DELETE.code || pIdx < 0 ? Op.INSERT : Op.UPDATE);
            // Superseding an existing instant: op reflects the original position's nature.
            nv.op = (pIdx < 0 || (p != null && p.op == Op.DELETE.code)) ? Op.INSERT.code : Op.UPDATE.code;
            chain.set(exact, nv);
            if (late) c.lateSplits++;
            c.updates++;
            return;
        }

        // No exact: does the predecessor already carry this value at vf?
        if (p != null && p.validTo > vf && p.op != Op.DELETE.code && p.attrHash == pr.attrHash) {
            c.noOps++;
            return;
        }

        long vt = n != null ? n.validFrom : Version.OPEN;
        Op op = (p == null || p.op == Op.DELETE.code) ? Op.INSERT : Op.UPDATE;
        if (p != null && p.validTo > vf) {
            p.validTo = vf;
            markDirty(p);
        }
        insertSorted(chain, newVersion(pr, vt, op));
        if (op == Op.INSERT) c.inserts++;
        else c.updates++;
        if (late) c.lateSplits++;
    }

    private WorkingVersion newVersion(PendingRow pr, long validTo, Op op) {
        WorkingVersion w = new WorkingVersion();
        w.validFrom = pr.validFrom;
        w.validTo = validTo;
        w.op = op.code;
        w.attrHash = pr.attrHash;
        w.row = pr.values;
        w.isNew = true;
        return w;
    }

    /** DELETE tombstone carrying the deleted version's payload for auditability. */
    private WorkingVersion tombstoneOf(TableRuntime rt, WorkingVersion prev, long vf, long validTo) {
        WorkingVersion w = new WorkingVersion();
        w.validFrom = vf;
        w.validTo = validTo;
        w.op = Op.DELETE.code;
        w.attrHash = prev.attrHash;
        w.row = decodeRow(rt, prev);
        w.isNew = true;
        return w;
    }

    private Map<String, Object> decodeRow(TableRuntime rt, WorkingVersion w) {
        if (w.row != null) return w.row;
        Codecs.DecodedValue dv = Codecs.decodeValue(w.storedValue, v -> rt.config().schemaAt(v));
        TableSchema written = rt.config().schemaAt(dv.schemaVersion());
        Map<String, Object> row = new LinkedHashMap<>();
        for (Column col : rt.config().currentSchema().columns()) {
            int idx = written.indexOf(col.name());
            row.put(col.name(), idx >= 0 ? dv.payload()[idx] : null);
        }
        return row;
    }

    private static void markDirty(WorkingVersion w) {
        if (!w.isNew) w.dirty = true;
    }

    private static int indexOfExact(List<WorkingVersion> chain, long vf) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).validFrom == vf) return i;
            if (chain.get(i).validFrom < vf) return -1;
        }
        return -1;
    }

    private static int indexOfFloorBelow(List<WorkingVersion> chain, long vf) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).validFrom < vf) return i;
        }
        return -1;
    }

    private static WorkingVersion nextAbove(List<WorkingVersion> chain, long vf) {
        for (WorkingVersion w : chain) {
            if (w.validFrom > vf) return w;
        }
        return null;
    }

    private static void insertSorted(List<WorkingVersion> chain, WorkingVersion w) {
        int i = 0;
        while (i < chain.size() && chain.get(i).validFrom < w.validFrom) i++;
        chain.add(i, w);
    }

    /** Writes new/dirty versions of one entity into the batch. */
    private void emit(TableRuntime rt, EntityRef ref, byte[] bkBytes, List<WorkingVersion> chain,
                      TableSchema schema, long txTime, long txnId, AtomicBatch out) {
        for (WorkingVersion w : chain) {
            if (!w.isNew && !w.dirty) continue;
            byte[] key = Codecs.dataKey(rt.tableId(), ref.keyHash(), ref.disambig(), w.validFrom, txTime);
            byte[] value;
            if (w.isNew) {
                value = Codecs.encodeValue(Op.fromCode(w.op), w.validTo, txTime, txnId,
                        schema.schemaVersion(), w.attrHash, bkBytes, schema, w.row);
            } else {
                value = Codecs.withNewValidTo(w.storedValue, w.validTo, txTime, txnId);
            }
            out.put(key, value);
        }
    }

    private record PendingRow(Map<String, Object> values, Map<String, Object> rawValues,
                              boolean delete, long validFrom, byte[] bkBytes, long attrHash) {}

    private record BytesKey(byte[] bytes) {
        @Override
        public boolean equals(Object o) {
            return o instanceof BytesKey b && Arrays.equals(bytes, b.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
