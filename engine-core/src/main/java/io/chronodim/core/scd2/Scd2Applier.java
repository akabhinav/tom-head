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

        public void add(Counters o) {
            rowsIn += o.rowsIn;
            inserts += o.inserts;
            updates += o.updates;
            noOps += o.noOps;
            deletes += o.deletes;
            lateSplits += o.lateSplits;
            rejects += o.rejects;
            quarantined += o.quarantined;
        }

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
        return applyTable(rt, rows, txTime, txnId, out, errors, quarantine, false);
    }

    public Counters applyTable(TableRuntime rt, List<InputRow> rows, long txTime, long txnId,
                               AtomicBatch out, ErrorSink errors, QuarantineSink quarantine, boolean fullSnapshot) {
        TableConfig cfg = rt.config();
        TableSchema schema = cfg.currentSchema();
        List<Column> bkCols = rt.businessKeyColumns();
        List<String> tracked = cfg.effectiveTrackedColumns();
        Counters c = new Counters();

        // Phase 1: coerce + validate + group by business key (preserving input order).
        Map<BytesKey, List<PendingRow>> groups = new LinkedHashMap<>();
        java.util.Set<BytesKey> seenKeys = new java.util.HashSet<>();
        long rowIndex = -1;
        for (InputRow in : rows) {
            rowIndex++;
            c.rowsIn++;
            try {
                PendingRow pr = coerce(cfg, schema, bkCols, tracked, in, txTime);
                // Snapshot semantics: a key present in the file — even with a failing
                // row — must never be deleted-by-absence.
                seenKeys.add(new BytesKey(pr.bkBytes));
                // Gates skip carried-forward columns: the inherited value passed
                // its gates when it was originally written.
                String gateFailure = gateFailure(cfg, pr.values, pr.absentCols());
                if (gateFailure != null) {
                    handleRowFailure(cfg, rt, in, c, errors, quarantine, rowIndex, gateFailure);
                    continue;
                }
                groups.computeIfAbsent(new BytesKey(pr.bkBytes), k -> new ArrayList<>()).add(pr);
            } catch (ValidationException e) {
                handleRowFailure(cfg, rt, in, c, errors, quarantine, rowIndex, e.getMessage());
            }
        }
        if (fullSnapshot) {
            for (byte[] bk : missingActiveKeys(rt, seenKeys)) {
                groups.computeIfAbsent(new BytesKey(bk), k -> new ArrayList<>())
                        .add(new PendingRow(new LinkedHashMap<>(), new LinkedHashMap<>(), true, txTime, bk, 0));
            }
        }

        // Phase 2a: resolve every entity's identity in bulk — one multiGet for all
        // keymap entries, one more for the hashreg probe of the misses (R-PERF-3).
        List<Map.Entry<BytesKey, List<PendingRow>>> entities = new ArrayList<>(groups.entrySet());
        EntityRef[] refs = resolveEntities(rt, entities, out);

        // Phase 2b: place rows per entity. Entities are disjoint by construction, so
        // large batches shard across cores by key hash (R-PERF-2); each shard owns its
        // batch + counters + seek iterator and everything merges deterministically.
        int shardCount = entities.size() >= PARALLEL_THRESHOLD ? POOL_SIZE : 1;
        if (shardCount <= 1) {
            try (StorageEngine.SeekIterator it = storage.seekIterator()) {
                for (int i = 0; i < entities.size(); i++) {
                    processEntity(rt, refs[i], entities.get(i), it, out, c, errors, quarantine, txTime, txnId);
                }
            }
        } else {
            ErrorSink syncErrors = (tbl, idx, reason) -> {
                synchronized (errors) {
                    errors.error(tbl, idx, reason);
                }
            };
            QuarantineSink syncQuar = (rt2, vals, del, reason) -> {
                synchronized (quarantine) {
                    quarantine.quarantine(rt2, vals, del, reason);
                }
            };
            List<List<Integer>> shards = new ArrayList<>(shardCount);
            for (int s = 0; s < shardCount; s++) shards.add(new ArrayList<>());
            for (int i = 0; i < entities.size(); i++) {
                shards.get((int) Long.remainderUnsigned(refs[i].keyHash(), shardCount)).add(i);
            }
            List<java.util.concurrent.Future<ShardResult>> futures = new ArrayList<>(shardCount);
            for (List<Integer> shard : shards) {
                futures.add(POOL.submit(() -> {
                    AtomicBatch shardOut = new AtomicBatch();
                    Counters shardC = new Counters();
                    try (StorageEngine.SeekIterator it = storage.seekIterator()) {
                        for (int i : shard) {
                            processEntity(rt, refs[i], entities.get(i), it, shardOut, shardC, syncErrors, syncQuar, txTime, txnId);
                        }
                    }
                    return new ShardResult(shardOut, shardC);
                }));
            }
            for (java.util.concurrent.Future<ShardResult> f : futures) {
                try {
                    ShardResult r = f.get();
                    out.mutations().addAll(r.batch.mutations());
                    c.add(r.counters);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new io.chronodim.api.ChronoDimException("interrupted during sharded apply", ie);
                } catch (java.util.concurrent.ExecutionException ee) {
                    if (ee.getCause() instanceof RuntimeException re) throw re;
                    // Errors (e.g. OutOfMemoryError on huge wide-row batches) land
                    // here — name the cause, "sharded apply failed" alone cost a
                    // debugging session once.
                    throw new io.chronodim.api.ChronoDimException(
                            "sharded apply failed: " + ee.getCause()
                            + (ee.getCause() instanceof OutOfMemoryError
                               ? " — batch too large for the heap; raise -Xmx or split the file" : ""),
                            ee.getCause());
                }
            }
        }
        return c;
    }

    private static final int PARALLEL_THRESHOLD = 2048; // entities per batch before sharding pays off
    private static final int POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final java.util.concurrent.ExecutorService POOL =
            java.util.concurrent.Executors.newFixedThreadPool(POOL_SIZE, r -> {
                Thread t = new Thread(r, "chronodim-apply-shard");
                t.setDaemon(true);
                return t;
            });

    private record ShardResult(AtomicBatch batch, Counters counters) {}

    private void processEntity(TableRuntime rt, EntityRef ref, Map.Entry<BytesKey, List<PendingRow>> entity,
                               StorageEngine.SeekIterator it, AtomicBatch out, Counters c,
                               ErrorSink errors, QuarantineSink quarantine, long txTime, long txnId) {
        TableConfig cfg = rt.config();
        byte[] bkBytes = entity.getKey().bytes;
        List<PendingRow> pending = dedupe(cfg, rt, entity.getValue(), c, errors, quarantine);
        if (pending.isEmpty()) return;

        List<WorkingVersion> chain = ref.isNew()
                ? new ArrayList<>()
                : loadChainLazy(rt, ref, pending.get(0).validFrom, it);
        for (PendingRow pr : pending) {
            placeRow(cfg, rt, chain, pr, c, errors, quarantine);
        }
        emit(rt, ref, bkBytes, chain, cfg.currentSchema(), txTime, txnId, out);
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
        return gateFailure(cfg, values, java.util.Set.of());
    }

    private static String gateFailure(TableConfig cfg, Map<String, Object> values, java.util.Set<String> skip) {
        for (QualityGate g : cfg.qualityGates()) {
            if (skip.contains(g.column())) continue;
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
        // CARRY_FORWARD: a column MISSING from the input (key absent, not explicit
        // null) inherits from the immediate previous version at placement time.
        java.util.Set<String> absent = cfg.absentColumns() == TableConfig.AbsentColumns.CARRY_FORWARD
                ? new java.util.LinkedHashSet<>() : java.util.Set.of();
        for (Column col : schema.columns()) {
            Object raw = in.values().get(col.name());
            if (cfg.absentColumns() == TableConfig.AbsentColumns.CARRY_FORWARD
                    && !in.values().containsKey(col.name()) && !in.delete()) {
                absent.add(col.name());
            }
            values.put(col.name(), col.type().coerce(raw));
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
        if (in.validFromMicrosOverride() != null) {
            validFrom = in.validFromMicrosOverride(); // envelope effective_at / SCD2 import wins
        } else if (cfg.validTimeMode() == TableConfig.ValidTimeMode.SOURCE_COLUMN) {
            Object v = values.get(cfg.validTimeColumn());
            if (v == null) throw new ValidationException("valid_time column '" + cfg.validTimeColumn() + "' is null");
            Column vc = schema.column(cfg.validTimeColumn());
            validFrom = vc.kind() == ColumnType.DATE ? (Integer) v * 86_400_000_000L : (Long) v;
        } else {
            validFrom = txTime;
        }
        byte[] bkBytes = Codecs.encodeBusinessKey(bkCols, bkValues);
        long attrHash = Codecs.attrHash(schema, tracked, values);
        return new PendingRow(values, in.values(), in.delete(), validFrom, bkBytes, attrHash, absent);
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

    /** Active entities of the table whose business key is absent from the batch (snapshot mode). */
    private List<byte[]> missingActiveKeys(TableRuntime rt, java.util.Set<BytesKey> seenKeys) {
        List<byte[]> missing = new ArrayList<>();
        long curHash = 0;
        int curDis = -1;
        boolean any = false;
        try (io.chronodim.storage.CloseableKvIterator it =
                     storage.prefixScan(Codecs.tableDataPrefix(rt.tableId()))) {
            while (it.hasNext()) {
                io.chronodim.storage.KV kv = it.next();
                Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                boolean newEntity = !any || k.keyHash() != curHash || k.disambig() != curDis;
                if (!newEntity) continue;
                any = true;
                curHash = k.keyHash();
                curDis = k.disambig();
                Codecs.DecodedValue h = Codecs.decodeHeader(kv.value());
                if (h.op() == Op.DELETE.code || h.validTo() != Version.OPEN) continue; // not active
                if (!seenKeys.contains(new BytesKey(h.bkBytes()))) missing.add(h.bkBytes());
            }
        }
        return missing;
    }

    // ---- entity identity ----------------------------------------------------------

    private record EntityRef(long keyHash, int disambig, boolean isNew) {}

    /**
     * Bulk identity resolution: one multiGet over all keymap entries, one more
     * over the hashreg slot-0 probes for the misses. New entities get their
     * keymap/hashreg puts appended here (single-threaded), so shards never write
     * identity records.
     */
    private EntityRef[] resolveEntities(TableRuntime rt, List<Map.Entry<BytesKey, List<PendingRow>>> entities,
                                        AtomicBatch out) {
        int n = entities.size();
        EntityRef[] refs = new EntityRef[n];
        List<byte[]> keymapKeys = new ArrayList<>(n);
        for (Map.Entry<BytesKey, List<PendingRow>> e : entities) {
            keymapKeys.add(Codecs.keymapKey(rt.tableId(), e.getKey().bytes));
        }
        List<byte[]> mapped = storage.multiGet(keymapKeys);

        List<Integer> misses = new ArrayList<>();
        long[] hashes = new long[n];
        for (int i = 0; i < n; i++) {
            byte[] m = mapped.get(i);
            if (m != null) {
                Codecs.KeyRef ref = Codecs.decodeKeymapValue(m);
                refs[i] = new EntityRef(ref.keyHash(), ref.disambig(), false);
            } else {
                hashes[i] = Codecs.businessKeyHash(entities.get(i).getKey().bytes);
                misses.add(i);
            }
        }
        if (misses.isEmpty()) return refs;

        List<byte[]> probes = new ArrayList<>(misses.size());
        for (int i : misses) probes.add(Codecs.hashregKey(rt.tableId(), hashes[i], 0));
        List<byte[]> slot0 = storage.multiGet(probes);

        Map<Long, java.util.Set<Integer>> claimedInBatch = new java.util.HashMap<>();
        for (int mi = 0; mi < misses.size(); mi++) {
            int i = misses.get(mi);
            byte[] bkBytes = entities.get(i).getKey().bytes;
            long hash = hashes[i];
            java.util.Set<Integer> claimedNow = claimedInBatch.computeIfAbsent(hash, h -> new java.util.HashSet<>());
            int disambig = 0;
            byte[] claimed = claimedNow.contains(0) ? null : slot0.get(mi);
            if (claimedNow.contains(0) || (claimed != null && !Arrays.equals(claimed, bkBytes))) {
                // Rare path: batch-internal or stored hash collision — probe upward.
                disambig = 1;
                while (true) {
                    if (!claimedNow.contains(disambig)) {
                        byte[] c2 = storage.get(Codecs.hashregKey(rt.tableId(), hash, disambig));
                        if (c2 == null || Arrays.equals(c2, bkBytes)) break;
                    }
                    disambig++;
                    if (disambig > 0xFFFF) throw new ValidationException("hash collision chain exhausted (impossible)");
                }
            }
            claimedNow.add(disambig);
            out.put(Codecs.keymapKey(rt.tableId(), bkBytes), Codecs.keymapValue(hash, disambig));
            out.put(Codecs.hashregKey(rt.tableId(), hash, disambig), bkBytes);
            refs[i] = new EntityRef(hash, disambig, true);
        }
        return refs;
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

    /**
     * Latest-knowledge chain, ascending validFrom — read lazily. The common CDC
     * case (every pending validFrom strictly after the latest version) needs only
     * the newest record; the full chain is decoded only for corrections, deletes
     * at historical instants, and same-instant supersedes.
     */
    private List<WorkingVersion> loadChainLazy(TableRuntime rt, EntityRef ref, long minPendingVf,
                                               StorageEngine.SeekIterator it) {
        List<WorkingVersion> chain = new ArrayList<>();
        byte[] prefix = Codecs.entityPrefix(rt.tableId(), ref.keyHash(), ref.disambig());
        KV kv = it.seekFirst(prefix);
        if (kv == null || !io.chronodim.storage.util.Bytes.hasPrefix(kv.key(), prefix)) {
            return chain; // keymap present but no data records (all-no-op history)
        }
        Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
        chain.add(WorkingVersion.fromStored(k, kv.value(), Codecs.decodeHeader(kv.value())));
        if (minPendingVf > k.validFrom()) {
            return chain; // append-only batch: the latest version is all placeRow needs
        }
        long lastVf = k.validFrom();
        while ((kv = it.next()) != null && io.chronodim.storage.util.Bytes.hasPrefix(kv.key(), prefix)) {
            k = Codecs.decodeDataKey(kv.key());
            if (k.validFrom() == lastVf) continue; // superseded belief
            lastVf = k.validFrom();
            chain.add(WorkingVersion.fromStored(k, kv.value(), Codecs.decodeHeader(kv.value())));
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

        // CARRY_FORWARD: columns absent from the input inherit from the IMMEDIATE
        // previous version at this row's effective position (exact-instant
        // supersede inherits from the version being superseded). The hash is
        // recomputed over the merged row, so no-op detection stays exact.
        if (!pr.delete && !pr.absentCols().isEmpty()) {
            WorkingVersion src = exact >= 0 ? chain.get(exact) : p;
            if (src != null && src.op != Op.DELETE.code) {
                Map<String, Object> prev = decodeRow(rt, src);
                Map<String, Object> merged = new LinkedHashMap<>(pr.values());
                for (String col : pr.absentCols()) {
                    Object inherited = prev.get(col);
                    if (inherited != null) merged.put(col, inherited);
                }
                pr = new PendingRow(merged, pr.rawValues(), false, vf, pr.bkBytes(),
                        Codecs.attrHash(cfg.currentSchema(), cfg.effectiveTrackedColumns(), merged));
            }
        }

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
                              boolean delete, long validFrom, byte[] bkBytes, long attrHash,
                              java.util.Set<String> absentCols) {
        PendingRow(Map<String, Object> values, Map<String, Object> rawValues,
                   boolean delete, long validFrom, byte[] bkBytes, long attrHash) {
            this(values, rawValues, delete, validFrom, bkBytes, attrHash, java.util.Set.of());
        }
    }

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
