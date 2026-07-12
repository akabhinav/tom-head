package io.chronodim.core.engine;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDimException;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.QuarantineEntry;
import io.chronodim.api.TableConfig;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableCatalog;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;
import io.chronodim.core.scd2.BackfillLoader;
import io.chronodim.core.scd2.Scd2Applier;
import io.chronodim.core.util.Json;
import io.chronodim.core.wal.TxnPayload;
import io.chronodim.core.wal.Wal;
import io.chronodim.core.wal.WalReader;
import io.chronodim.core.wal.WalWriter;
import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.KvSnapshot;
import io.chronodim.storage.StorageEngine;
import io.chronodim.storage.lsm.LsmStorageEngine;
import io.chronodim.storage.rocks.RocksDbStorageEngine;
import io.chronodim.storage.util.XxHash64;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The engine: single-writer commit pipeline over WAL + StorageEngine, with
 * snapshot-consistent readers (§6). See docs/formats.md for every on-disk
 * contract this class relies on.
 *
 * <p>Commit protocol per transaction: (1) build the mutation set against the
 * latest state under the apply mutex, (2) append one TXN_COMMIT record, (3) apply
 * the mutations to storage, (4) release the mutex, (5) block until the group-
 * committed fsync covers the record, (6) notify plugins. A crash before (5)
 * replays from the WAL; storage never runs ahead of the log.
 */
public final class EngineImpl implements Engine {

    public static final String WAL_DIR = "wal";
    public static final String STORAGE_DIR = "storage";
    public static final String TMP_DIR = "tmp";
    public static final String SNAPSHOT_DIR = "snapshots";

    private static final byte META_SUB_COUNTER = 2;
    private static final String QUAR_SEQ = "quar_seq";

    private final Path dataDir;
    private final EngineOptions options;
    private final EngineLock lock;
    private final StorageEngine storage;
    private final WalWriter wal;
    private final TableCatalog catalog = new TableCatalog();
    private final Scd2Applier applier;
    private final List<EnginePlugin> plugins = new ArrayList<>();
    private final ReentrantLock applyMutex = new ReentrantLock(true);

    private long nextTxn;
    private long lastTxTime;
    private long quarSeq;
    private volatile long lastCommitted = -1;
    private volatile boolean closed;

    private final AtomicLong applies = new AtomicLong();
    private final AtomicLong rowsIn = new AtomicLong();

    public static EngineImpl open(Path dataDir, EngineOptions options) {
        return new EngineImpl(dataDir, options);
    }

    private EngineImpl(Path dataDir, EngineOptions options) {
        this.dataDir = dataDir;
        this.options = options;
        try {
            Files.createDirectories(dataDir);
            Files.createDirectories(dataDir.resolve(WAL_DIR));
            Files.createDirectories(dataDir.resolve(TMP_DIR));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.lock = new EngineLock(dataDir);
        boolean ok = false;
        try {
            this.storage = openStorage(dataDir.resolve(STORAGE_DIR), options);
            long durable = storage.durableTxn();

            // Recovery (R-WAL-3/5): replay committed transactions newer than what
            // storage already contains. Mutations are raw KV bytes → idempotent.
            long walMax = WalReader.scan(dataDir.resolve(WAL_DIR), true, rec -> {
                if ((rec.type() == Wal.TXN_COMMIT || rec.type() == Wal.CONFIG_CHANGE) && rec.txnId() > durable) {
                    TxnPayload.Decoded d = TxnPayload.decode(rec.payload());
                    storage.write(d.batch(), rec.txnId());
                }
            });
            this.nextTxn = Math.max(walMax, durable) + 1;
            this.lastCommitted = nextTxn - 1;
            this.catalog.load(storage);
            byte[] qs = storage.get(Codecs.metaKey(META_SUB_COUNTER, QUAR_SEQ));
            this.quarSeq = qs == null ? 0 : ByteBuffer.wrap(qs).getLong();
            this.wal = new WalWriter(dataDir.resolve(WAL_DIR), options.walSegmentBytes(), options.fsync(),
                    options.groupCommitWindowMicros(), options.groupCommitMaxTxns(), nextTxn);
            this.applier = new Scd2Applier(storage);
            for (EnginePlugin p : ServiceLoader.load(EnginePlugin.class)) {
                if (p.attach(this)) plugins.add(p);
            }
            ok = true;
        } finally {
            if (!ok) lock.close();
        }
    }

    private static StorageEngine openStorage(Path dir, EngineOptions o) {
        return switch (o.storageBackend()) {
            case ROCKSDB -> new RocksDbStorageEngine(dir, o.memtableFlushBytes(), o.blockCacheBytes());
            case LSM -> new LsmStorageEngine(dir, o.memtableFlushBytes(), o.maxSegmentsBeforeCompaction(), o.fsync());
        };
    }

    // ---- accessors for plugins (export / durability modules) ---------------------

    public Path dataDir() { return dataDir; }
    public Path walDir() { return dataDir.resolve(WAL_DIR); }
    public EngineOptions options() { return options; }
    public TableCatalog catalog() { return catalog; }
    public StorageEngine storage() { return storage; }

    /** Appends an informational WAL marker (SNAPSHOT_MARK / PUBLISH_MARK). */
    public void appendMark(byte type, String json) {
        wal.append(type, lastCommitted, json.getBytes(StandardCharsets.UTF_8));
    }

    // ---- catalog --------------------------------------------------------------------

    @Override
    public void createTable(TableConfig config) {
        configChange(batch -> {
            TableRuntime rt = catalog.prepareCreate(config, batch);
            return rt;
        });
    }

    @Override
    public void alterTable(TableConfig config) {
        configChange(batch -> catalog.prepareAlter(config, batch));
    }

    private void configChange(java.util.function.Function<AtomicBatch, TableRuntime> prepare) {
        ensureOpen();
        WalWriter.Appended app;
        applyMutex.lock();
        try {
            AtomicBatch batch = new AtomicBatch();
            TableRuntime rt = prepare.apply(batch);
            long txn = nextTxn;
            app = wal.append(Wal.CONFIG_CHANGE, txn, TxnPayload.encode(batch, "{}"));
            storage.write(batch, txn);
            catalog.register(rt.config(), rt.tableId());
            nextTxn = txn + 1;
            lastCommitted = txn;
        } finally {
            applyMutex.unlock();
        }
        wal.awaitDurable(app.seq());
        notifyPlugins(lastCommitted);
    }

    @Override
    public List<String> listTables() {
        return catalog.names();
    }

    @Override
    public TableConfig describeTable(String table) {
        return catalog.get(table).config();
    }

    // ---- write path -------------------------------------------------------------------

    @Override
    public AuditManifest apply(ApplyBatch req) {
        return applyInternal(req.loadId(), req.tables(), b -> {});
    }

    private AuditManifest applyInternal(String loadId, List<ApplyBatch.TableBatch> tableBatches,
                                        java.util.function.Consumer<AtomicBatch> extraMutations) {
        ensureOpen();
        long startNanos = System.nanoTime();
        WalWriter.Appended app;
        AuditManifest manifest;
        applyMutex.lock();
        try {
            AuditManifest prior = findManifestByLoadId(loadId);
            if (prior != null) return prior;

            long txn = nextTxn;
            long txTime = nextTxTime();
            AtomicBatch batch = new AtomicBatch();
            BoundedErrors errors = new BoundedErrors(options.maxManifestErrors());
            QuarantineBuffer quar = new QuarantineBuffer();
            List<AuditManifest.TableStats> stats = new ArrayList<>();

            for (ApplyBatch.TableBatch tb : tableBatches) {
                TableRuntime rt = catalog.get(tb.table());
                Scd2Applier.Counters c = applier.applyTable(rt, tb.rows(), txTime, txn, batch, errors, quar);
                stats.add(c.toStats(rt.config().table(), rt.configHash(), rt.config().currentSchema().schemaVersion()));
                rowsIn.addAndGet(c.rowsIn);
            }
            quar.materialize(batch, loadId, txn, txTime);
            extraMutations.accept(batch);

            long wallMillis = (System.nanoTime() - startNanos) / 1_000_000;
            ManifestHolder holder = new ManifestHolder();
            app = wal.append(Wal.TXN_COMMIT, txn, (segment, offset) -> {
                AuditManifest m = new AuditManifest(loadId, txn, txTime, wallMillis, segment, offset, 0,
                        false, false, stats, errors.list);
                String json = ManifestCodec.toJson(m);
                batch.put(Codecs.manifestKey(txn), json.getBytes(StandardCharsets.UTF_8));
                batch.put(Codecs.loadIdKey(loadId), ByteBuffer.allocate(8).putLong(txn).array());
                holder.manifest = m;
                return TxnPayload.encode(batch, json);
            });
            storage.write(batch, txn);
            nextTxn = txn + 1;
            lastCommitted = txn;
            manifest = withEndOffset(holder.manifest, app.endOffset());
        } finally {
            applyMutex.unlock();
        }
        wal.awaitDurable(app.seq());
        applies.incrementAndGet();
        notifyPlugins(manifest.txnId());
        return manifest;
    }

    @Override
    public AuditManifest backfill(String loadId, String table, Iterable<InputRow> rows) {
        ensureOpen();
        long startNanos = System.nanoTime();
        WalWriter.Appended app;
        AuditManifest manifest;
        applyMutex.lock();
        try {
            AuditManifest prior = findManifestByLoadId(loadId);
            if (prior != null) return prior;

            TableRuntime rt = catalog.get(table);
            try (CloseableKvIterator it = storage.prefixScan(Codecs.keymapKey(rt.tableId(), new byte[0]))) {
                if (it.hasNext()) {
                    throw new ValidationException("backfill requires an empty table, but '" + table + "' has data"
                            + " (use apply for incremental changes)");
                }
            }

            long txnA = nextTxn++;
            long txTime = nextTxTime();
            String fence = Json.write(Map.of("table", table, "load_id", loadId));
            WalWriter.Appended begin = wal.append(Wal.BACKFILL_BEGIN, txnA, fence.getBytes(StandardCharsets.UTF_8));
            wal.awaitDurable(begin.seq()); // the fence must be on disk before bulk data

            BoundedErrors errors = new BoundedErrors(options.maxManifestErrors());
            Path tmp = dataDir.resolve(TMP_DIR).resolve("backfill-" + txnA);
            Scd2Applier.Counters c;
            try {
                c = BackfillLoader.load(rt, rows, txTime, txnA, storage, tmp, 256L << 20, errors);
            } finally {
                deleteRecursive(tmp);
            }
            rowsIn.addAndGet(c.rowsIn);

            long txnB = nextTxn++;
            AtomicBatch batch = new AtomicBatch();
            wal.append(Wal.BACKFILL_END, txnB, fence.getBytes(StandardCharsets.UTF_8));
            long wallMillis = (System.nanoTime() - startNanos) / 1_000_000;
            List<AuditManifest.TableStats> stats = List.of(
                    c.toStats(rt.config().table(), rt.configHash(), rt.config().currentSchema().schemaVersion()));
            ManifestHolder holder = new ManifestHolder();
            app = wal.append(Wal.TXN_COMMIT, txnB, (segment, offset) -> {
                AuditManifest m = new AuditManifest(loadId, txnB, txTime, wallMillis, segment, offset, 0,
                        false, true, stats, errors.list);
                String json = ManifestCodec.toJson(m);
                batch.put(Codecs.manifestKey(txnB), json.getBytes(StandardCharsets.UTF_8));
                batch.put(Codecs.loadIdKey(loadId), ByteBuffer.allocate(8).putLong(txnB).array());
                holder.manifest = m;
                return TxnPayload.encode(batch, json);
            });
            storage.write(batch, txnB);
            lastCommitted = txnB;
            manifest = withEndOffset(holder.manifest, app.endOffset());
        } finally {
            applyMutex.unlock();
        }
        wal.awaitDurable(app.seq());
        applies.incrementAndGet();
        notifyPlugins(manifest.txnId());
        return manifest;
    }

    // ---- read path ---------------------------------------------------------------------

    @Override
    public Optional<Version> getCurrent(String table, Map<String, Object> businessKey) {
        TableRuntime rt = catalog.get(table);
        try (KvSnapshot snap = storage.snapshot()) {
            return ReadOps.getCurrent(snap, rt, businessKey);
        }
    }

    @Override
    public Optional<Version> getAsOf(String table, Map<String, Object> businessKey, long validTimeMicros) {
        TableRuntime rt = catalog.get(table);
        try (KvSnapshot snap = storage.snapshot()) {
            return ReadOps.getAsOf(snap, rt, businessKey, validTimeMicros);
        }
    }

    @Override
    public Optional<Version> getAsOf(String table, Map<String, Object> businessKey, long validTimeMicros, long txTimeMicros) {
        TableRuntime rt = catalog.get(table);
        try (KvSnapshot snap = storage.snapshot()) {
            return ReadOps.getAsOf(snap, rt, businessKey, validTimeMicros, txTimeMicros);
        }
    }

    @Override
    public List<Version> getHistory(String table, Map<String, Object> businessKey) {
        TableRuntime rt = catalog.get(table);
        try (KvSnapshot snap = storage.snapshot()) {
            return ReadOps.getHistory(snap, rt, businessKey);
        }
    }

    @Override
    public VersionCursor scanCurrent(String table) {
        TableRuntime rt = catalog.get(table);
        return ReadOps.scanCurrent(storage.snapshot(), rt); // cursor owns the snapshot
    }

    @Override
    public VersionCursor scanAsOf(String table, long validTimeMicros) {
        TableRuntime rt = catalog.get(table);
        return ReadOps.scanAsOf(storage.snapshot(), rt, validTimeMicros);
    }

    // ---- manifests / quarantine ------------------------------------------------------------

    @Override
    public Optional<AuditManifest> manifest(String loadId) {
        return Optional.ofNullable(findManifestByLoadId(loadId));
    }

    private AuditManifest findManifestByLoadId(String loadId) {
        byte[] txnBytes = storage.get(Codecs.loadIdKey(loadId));
        if (txnBytes == null) return null;
        long txn = ByteBuffer.wrap(txnBytes).getLong();
        byte[] json = storage.get(Codecs.manifestKey(txn));
        if (json == null) throw new ChronoDimException("load_id '" + loadId + "' recorded but manifest for txn " + txn + " missing");
        return ManifestCodec.fromJson(new String(json, StandardCharsets.UTF_8), true);
    }

    @Override
    public List<AuditManifest> listManifests(String table, int limit) {
        ArrayDeque<AuditManifest> ring = new ArrayDeque<>(limit);
        try (CloseableKvIterator it = storage.prefixScan(new byte[]{Codecs.KS_MANIFEST})) {
            while (it.hasNext()) {
                KV kv = it.next();
                AuditManifest m = ManifestCodec.fromJson(new String(kv.value(), StandardCharsets.UTF_8), false);
                if (table != null && m.tables().stream().noneMatch(t -> t.table().equals(table))) continue;
                if (ring.size() == limit) ring.pollFirst();
                ring.addLast(m);
            }
        }
        List<AuditManifest> out = new ArrayList<>(ring);
        java.util.Collections.reverse(out); // newest first
        return out;
    }

    @Override
    public List<QuarantineEntry> quarantineList(String table, int limit) {
        TableRuntime rt = catalog.get(table);
        List<QuarantineEntry> out = new ArrayList<>();
        try (CloseableKvIterator it = storage.prefixScan(Codecs.quarPrefix(rt.tableId()))) {
            while (it.hasNext() && out.size() < limit) {
                out.add(decodeQuarantine(new String(it.next().value(), StandardCharsets.UTF_8)));
            }
        }
        return out;
    }

    @Override
    public AuditManifest quarantineReapply(String table, List<Long> ids, String loadId) {
        TableRuntime rt = catalog.get(table);
        List<InputRow> rows = new ArrayList<>();
        List<byte[]> keysToDelete = new ArrayList<>();
        try (CloseableKvIterator it = storage.prefixScan(Codecs.quarPrefix(rt.tableId()))) {
            while (it.hasNext()) {
                KV kv = it.next();
                QuarantineEntry e = decodeQuarantine(new String(kv.value(), StandardCharsets.UTF_8));
                if (ids != null && !ids.isEmpty() && !ids.contains(e.id())) continue;
                rows.add(new InputRow(e.values(), e.delete()));
                keysToDelete.add(kv.key());
            }
        }
        if (rows.isEmpty()) throw new ValidationException("no quarantined rows matched for table '" + table + "'");
        return applyInternal(loadId, List.of(new ApplyBatch.TableBatch(table, rows)),
                batch -> keysToDelete.forEach(batch::delete));
    }

    @SuppressWarnings("unchecked")
    private QuarantineEntry decodeQuarantine(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new QuarantineEntry(
                ((Number) m.get("id")).longValue(),
                (String) m.get("table"),
                (String) m.get("load_id"),
                ((Number) m.get("txn_id")).longValue(),
                ((Number) m.get("at_micros")).longValue(),
                (String) m.get("reason"),
                Boolean.TRUE.equals(m.get("delete")),
                (Map<String, Object>) m.get("values"));
    }

    // ---- operations ------------------------------------------------------------------------

    @Override
    public Map<String, Object> verify() {
        ensureOpen();
        Map<Integer, String> idToName = new LinkedHashMap<>();
        for (TableRuntime rt : catalog.all()) idToName.put(rt.tableId(), rt.config().table());

        long fingerprint = 0;
        Map<String, long[]> perTable = new LinkedHashMap<>(); // {records, entities, current}
        try (KvSnapshot snap = storage.snapshot()) {
            try (CloseableKvIterator it = snap.prefixScan(new byte[]{Codecs.KS_DATA})) {
                long curHash = 0;
                int curDis = -1;
                int curTable = -1;
                while (it.hasNext()) {
                    KV kv = it.next();
                    fingerprint += XxHash64.hash(kv.key()) ^ XxHash64.hashLong(XxHash64.hash(kv.value()), 0x5bd1e995);
                    Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                    long[] t = perTable.computeIfAbsent(
                            idToName.getOrDefault(k.tableId(), "#" + k.tableId()), x -> new long[3]);
                    t[0]++;
                    boolean newEntity = k.tableId() != curTable || k.keyHash() != curHash || k.disambig() != curDis;
                    if (newEntity) {
                        curTable = k.tableId();
                        curHash = k.keyHash();
                        curDis = k.disambig();
                        t[1]++;
                        Codecs.DecodedValue h = Codecs.decodeHeader(kv.value());
                        if (h.op() != io.chronodim.api.Op.DELETE.code && h.validTo() == Version.OPEN) t[2]++;
                    }
                }
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("state_fingerprint", String.format("%016x", fingerprint));
        out.put("last_committed_txn", lastCommitted);
        out.put("durable_txn", storage.durableTxn());
        Map<String, Object> tables = new LinkedHashMap<>();
        perTable.forEach((name, v) -> {
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("version_records", v[0]);
            tm.put("entities", v[1]);
            tm.put("current_rows", v[2]);
            tables.put(name, tm);
        });
        out.put("tables", tables);
        for (EnginePlugin p : plugins) out.putAll(p.stats());
        return out;
    }

    @Override
    public Map<String, Object> snapshot() {
        ensureOpen();
        long txn = lastCommitted;
        Path dir = dataDir.resolve(SNAPSHOT_DIR).resolve("checkpoint-" + txn);
        if (Files.exists(dir)) deleteRecursive(dir);
        storage.checkpoint(dir);
        appendMark(Wal.SNAPSHOT_MARK, Json.write(Map.of("txn_id", txn, "path", dir.toString())));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("txn_id", txn);
        out.put("path", dir.toString());
        return out;
    }

    @Override
    public Map<String, Object> pruneWal() {
        ensureOpen();
        long durable = storage.durableTxn();
        List<java.nio.file.Path> segments = WalReader.segments(walDir());

        List<String> pruned = new ArrayList<>();
        List<String> kept = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            java.nio.file.Path seg = segments.get(i);
            String name = seg.getFileName().toString();
            if (i == segments.size() - 1) {
                kept.add(name + " (active)");
                continue;
            }
            // Closed segments only — the active one may be mid-append right now.
            long maxTxn = WalReader.scanSegment(seg, false, false, rec -> {});
            if (maxTxn > durable) {
                kept.add(name + " (beyond durable watermark " + durable + ")");
                continue;
            }
            boolean vetoed = false;
            for (EnginePlugin p : plugins) {
                if (!p.allowWalPrune(name, maxTxn)) {
                    kept.add(name + " (held by " + p.getClass().getSimpleName() + ")");
                    vetoed = true;
                    break;
                }
            }
            if (vetoed) continue;
            try {
                Files.deleteIfExists(seg);
                pruned.add(name);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot prune WAL segment " + seg, e);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("durable_txn", durable);
        out.put("pruned", pruned);
        out.put("kept", kept);
        return out;
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("applies", applies.get());
        m.put("rows_in", rowsIn.get());
        m.put("last_committed_txn", lastCommitted);
        m.put("tables", (long) catalog.names().size());
        m.put("storage", storage.stats());
        m.put("wal", wal.stats());
        for (EnginePlugin p : plugins) m.putAll(p.stats());
        return m;
    }

    @Override
    public long lastCommittedTxn() {
        return lastCommitted;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        ChronoDimException failure = null;
        for (EnginePlugin p : plugins) {
            try {
                p.close();
            } catch (Exception e) {
                failure = new ChronoDimException("plugin close failed", e);
            }
        }
        try {
            wal.close();
        } finally {
            try {
                storage.close();
            } finally {
                lock.close();
            }
        }
        if (failure != null) throw failure;
    }

    // ---- internals ------------------------------------------------------------------------------

    private long nextTxTime() {
        Instant now = Instant.now();
        long micros = now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
        lastTxTime = Math.max(micros, lastTxTime + 1); // tx time is strictly monotonic (§5.3)
        return lastTxTime;
    }

    private void notifyPlugins(long txn) {
        for (EnginePlugin p : plugins) p.onCommit(txn);
    }

    private void ensureOpen() {
        if (closed) throw new ChronoDimException("engine is closed");
    }

    private static AuditManifest withEndOffset(AuditManifest m, long endOffset) {
        return new AuditManifest(m.loadId(), m.txnId(), m.startedAtMicros(), m.wallClockMillis(),
                m.walSegment(), m.walStartOffset(), endOffset, m.alreadyApplied(), m.backfill(),
                m.tables(), m.errors());
    }

    private static void deleteRecursive(Path p) {
        if (!Files.exists(p)) return;
        try (var walk = Files.walk(p)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.deleteIfExists(f);
                } catch (IOException ignored) {
                    // best-effort temp cleanup
                }
            });
        } catch (IOException ignored) {
            // best-effort temp cleanup
        }
    }

    private static final class ManifestHolder {
        AuditManifest manifest;
    }

    private static final class BoundedErrors implements Scd2Applier.ErrorSink {
        final List<AuditManifest.RowError> list = new ArrayList<>();
        final int max;

        BoundedErrors(int max) {
            this.max = max;
        }

        @Override
        public void error(String table, long rowIndex, String reason) {
            if (list.size() < max) list.add(new AuditManifest.RowError(table, rowIndex, reason));
        }
    }

    private final class QuarantineBuffer implements Scd2Applier.QuarantineSink {
        private final List<Object[]> entries = new ArrayList<>();

        @Override
        public void quarantine(TableRuntime rt, Map<String, Object> rawValues, boolean delete, String reason) {
            entries.add(new Object[]{rt, rawValues, delete, reason});
        }

        void materialize(AtomicBatch batch, String loadId, long txn, long txTime) {
            if (entries.isEmpty()) return;
            for (Object[] e : entries) {
                TableRuntime rt = (TableRuntime) e[0];
                long id = ++quarSeq;
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("id", id);
                json.put("table", rt.config().table());
                json.put("load_id", loadId);
                json.put("txn_id", txn);
                json.put("at_micros", txTime);
                json.put("reason", e[3]);
                json.put("delete", e[2]);
                json.put("values", e[1]);
                batch.put(Codecs.quarKey(rt.tableId(), id), Json.write(json).getBytes(StandardCharsets.UTF_8));
            }
            batch.put(Codecs.metaKey(META_SUB_COUNTER, QUAR_SEQ), ByteBuffer.allocate(8).putLong(quarSeq).array());
        }
    }

}
