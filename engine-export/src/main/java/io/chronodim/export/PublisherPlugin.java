package io.chronodim.export;

import io.chronodim.api.Column;
import io.chronodim.api.Op;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;
import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.engine.EnginePlugin;
import io.chronodim.core.util.Json;
import io.chronodim.core.wal.TxnPayload;
import io.chronodim.core.wal.Wal;
import io.chronodim.core.wal.WalReader;
import io.chronodim.storage.AtomicBatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Cold-tier publisher (R-PUB-1/2/5): after every committed transaction, appends
 * that transaction's new version rows to each published table's location in the
 * frozen open column contract ({@link PublishedContract}), with an atomic commit
 * log providing exactly-once semantics across crashes.
 *
 * <p>Source of truth is the engine WAL: the publisher tails TXN_COMMIT records
 * strictly after each table's published watermark, so a crash between hot commit
 * and publish causes re-publish, never duplication (the watermark advances only
 * with the log entry).
 *
 * <p>The v1 part format is JSON Lines — readable by DuckDB
 * ({@code read_json_auto}), Spark, Databricks and Polars with zero dependencies.
 * A Delta Kernel/Parquet writer drops in behind {@link PartWriter} without
 * changing the contract.
 */
public final class PublisherPlugin implements EnginePlugin {

    private EngineImpl engine;
    private final Map<String, TableTarget> targets = new LinkedHashMap<>();
    private final Object signal = new Object();
    private Thread worker;
    private volatile boolean running;
    private volatile long lastPublishAtMillis;
    private volatile long pendingCommits;

    private static final class TableTarget {
        final TableRuntime rt;
        final Path location;
        final PublishedContract.Style style;
        long publishedTxn = -1;
        long logSeq = -1;

        TableTarget(TableRuntime rt, Path location) {
            this.rt = rt;
            this.location = location;
            this.style = PublishedContract.Style.forConfig(rt.config().publish());
        }
    }

    @Override
    public boolean attach(EngineImpl engine) {
        if (!engine.options().publishEnabled()) return false;
        this.engine = engine;
        syncTargets();
        running = true;
        worker = Thread.ofVirtual().name("chronodim-publisher").start(this::loop); // background I/O only (R-PERF-4)
        return true;
    }

    /** Picks up publish-enabled tables, including ones created after open. */
    private void syncTargets() {
        for (TableRuntime rt : engine.catalog().all()) {
            TableConfig.PublishConfig pub = rt.config().publish();
            if (!pub.enabled() || targets.containsKey(rt.config().table())) continue;
            TableTarget t = new TableTarget(rt, resolveLocation(pub.location()));
            initTarget(t);
            targets.put(rt.config().table(), t);
        }
    }

    /** file:///path, file:/path or a plain path. Object-store URIs need the durability shipper. */
    public static Path resolveLocation(String location) {
        String loc = location;
        if (loc.startsWith("file://")) loc = loc.substring("file://".length());
        else if (loc.startsWith("file:")) loc = loc.substring("file:".length());
        if (loc.contains("://")) {
            throw new io.chronodim.api.ConfigException("publish.location '" + location
                    + "': only local/mounted filesystem paths are supported by the v1 publisher"
                    + " (mount the bucket, or ship with the durability module)");
        }
        return Path.of(loc);
    }

    private void initTarget(TableTarget t) {
        try {
            Files.createDirectories(t.location.resolve(PublishedContract.DATA_DIR));
            Files.createDirectories(t.location.resolve(PublishedContract.LOG_DIR));
            Set<String> referenced = new HashSet<>();
            // Recover watermark from the commit log (R-PUB-2).
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(t.location.resolve(PublishedContract.LOG_DIR), "*.json")) {
                TreeMap<Long, Map<String, Object>> entries = new TreeMap<>();
                for (Path p : ds) {
                    String name = p.getFileName().toString();
                    long seq = Long.parseLong(name.substring(0, name.length() - 5));
                    entries.put(seq, Json.parseObject(Files.readString(p)));
                }
                for (Map.Entry<Long, Map<String, Object>> e : entries.entrySet()) {
                    t.logSeq = e.getKey();
                    Object to = e.getValue().get("txn_to");
                    if (to != null) t.publishedTxn = Math.max(t.publishedTxn, ((Number) to).longValue());
                    Object files = e.getValue().get("files");
                    if (files instanceof List<?> l) {
                        for (Object f : l) referenced.add(String.valueOf(((Map<?, ?>) f).get("path")));
                    }
                }
            }
            // Remove parts orphaned by a crash between file write and log write
            // (recursive: partitioned tables nest parts in col=value directories).
            Path dataDir = t.location.resolve(PublishedContract.DATA_DIR);
            try (var walk = Files.walk(dataDir)) {
                for (Path p : walk.toList()) {
                    if (Files.isDirectory(p)) continue;
                    String name = p.getFileName().toString();
                    if (!name.startsWith("part-") || !name.endsWith(".jsonl")) continue;
                    String rel = t.location.relativize(p).toString().replace('\\', '/');
                    if (!referenced.contains(rel)) {
                        Files.deleteIfExists(p);
                    }
                }
            }
            // Ship the view template + contract descriptor once.
            boolean partitioned = !t.rt.config().publish().partitionBy().isEmpty();
            Path view = t.location.resolve("scd2_view.sql");
            if (!Files.exists(view)) {
                String glob = partitioned ? "/data/**/*.jsonl" : "/data/*.jsonl";
                Files.writeString(view, PublishedContract.viewTemplate(
                        t.rt.config().table(),
                        "read_json_auto('" + t.location.toAbsolutePath() + glob + "')",
                        t.rt.config().businessKey(), t.style));
            }
            Path contract = t.location.resolve("_contract.json");
            if (!Files.exists(contract)) {
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("format", "chronodim-open-v1");
                c.put("table", t.rt.config().table());
                c.put("business_key", t.rt.config().businessKey());
                List<Map<String, Object>> cols = new ArrayList<>();
                for (Column col : t.rt.config().currentSchema().columns()) {
                    cols.add(Map.of("name", col.name(), "type", col.typeDeclaration()));
                }
                c.put("columns", cols);
                List<String> sys = new ArrayList<>(List.of(t.style.startCol(), t.style.endCol(),
                        PublishedContract.TX_TIME, PublishedContract.TXN_ID));
                if (t.style.includeOps()) {
                    sys.add(PublishedContract.OP);
                    sys.add(PublishedContract.SCHEMA_VERSION);
                }
                c.put("system_columns", sys);
                c.put("column_style", t.style.name());
                c.put("partition_by", t.rt.config().publish().partitionBy());
                Files.writeString(contract, Json.writePretty(c));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot initialise publish location " + t.location, e);
        }
    }

    @Override
    public void onCommit(long txnId) {
        pendingCommits++;
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    private void loop() {
        while (running) {
            try {
                synchronized (signal) {
                    if (pendingCommits == 0) {
                        signal.wait(engine.options().publishIntervalMillis());
                    }
                    pendingCommits = 0;
                }
                publishCycle();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                System.err.println("[chronodim-publisher] publish cycle failed: " + e);
            }
        }
    }

    /** Publishes everything committed after each target's watermark. Synchronized for close(). */
    synchronized void publishCycle() {
        syncTargets();
        if (targets.isEmpty()) return;
        long target = engine.lastCommittedTxn();
        long minWatermark = Long.MAX_VALUE;
        for (TableTarget t : targets.values()) minWatermark = Math.min(minWatermark, t.publishedTxn);
        if (target <= minWatermark) return;

        Map<Integer, TableTarget> byId = new HashMap<>();
        for (TableTarget t : targets.values()) byId.put(t.rt.tableId(), t);

        Map<Integer, List<Map<String, Object>>> rows = new HashMap<>();
        Map<Integer, long[]> ranges = new HashMap<>(); // txn_from, txn_to
        Map<Integer, List<String>> loadIds = new HashMap<>();
        final long watermarkFloor = minWatermark;

        WalReader.scan(engine.walDir(), false, rec -> {
            if (rec.txnId() <= watermarkFloor || rec.txnId() > target) return;
            if (rec.type() != Wal.TXN_COMMIT && rec.type() != Wal.CONFIG_CHANGE) return;
            TxnPayload.Decoded d = TxnPayload.decode(rec.payload());
            String loadId = loadIdOf(d.manifestJson());
            for (AtomicBatch.Mutation m : d.batch().mutations()) {
                if (!(m instanceof AtomicBatch.Put p) || p.key().length == 0 || p.key()[0] != Codecs.KS_DATA) continue;
                Codecs.DataKey k = Codecs.decodeDataKey(p.key());
                TableTarget t = byId.get(k.tableId());
                if (t == null || rec.txnId() <= t.publishedTxn) continue;
                Map<String, Object> row = renderRow(t.rt, k, p.value(), t.style);
                if (row == null) continue; // tombstone, not published in this style
                rows.computeIfAbsent(k.tableId(), x -> new ArrayList<>()).add(row);
                ranges.merge(k.tableId(), new long[]{rec.txnId(), rec.txnId()},
                        (a, b) -> new long[]{Math.min(a[0], b[0]), Math.max(a[1], b[1])});
                if (loadId != null) loadIds.computeIfAbsent(k.tableId(), x -> new ArrayList<>()).add(loadId);
            }
        });

        for (TableTarget t : targets.values()) {
            List<Map<String, Object>> tableRows = rows.get(t.rt.tableId());
            if (tableRows == null || tableRows.isEmpty()) {
                // Nothing for this table in (watermark, target]; advance silently via a
                // log-less watermark? No — watermark only moves with a log entry, so an
                // empty range simply stays put (re-scanned next time, cheap).
                continue;
            }
            long[] range = ranges.get(t.rt.tableId());
            writePart(t, tableRows, range[0], target, loadIds.getOrDefault(t.rt.tableId(), List.of()));
        }
        lastPublishAtMillis = System.currentTimeMillis();
    }

    private void writePart(TableTarget t, List<Map<String, Object>> rows, long txnFrom, long txnTo, List<String> loadIds) {
        try {
            long seq = t.logSeq + 1;
            List<String> partitionBy = t.rt.config().publish().partitionBy();

            // Hive-style partitioning: one part per touched partition per cycle.
            Map<String, List<Map<String, Object>>> byPartition = new LinkedHashMap<>();
            for (Map<String, Object> r : rows) {
                byPartition.computeIfAbsent(PublishedContract.partitionPath(partitionBy, r), x -> new ArrayList<>()).add(r);
            }

            String partName = String.format("part-%020d-%020d.jsonl", txnFrom, txnTo);
            List<Map<String, Object>> files = new ArrayList<>();
            for (Map.Entry<String, List<Map<String, Object>>> pe : byPartition.entrySet()) {
                Path dir = t.location.resolve(PublishedContract.DATA_DIR);
                String relDir = PublishedContract.DATA_DIR;
                if (!pe.getKey().isEmpty()) {
                    dir = dir.resolve(pe.getKey());
                    relDir = relDir + "/" + pe.getKey();
                }
                Files.createDirectories(dir);
                StringBuilder sb = new StringBuilder(pe.getValue().size() * 128);
                for (Map<String, Object> r : pe.getValue()) sb.append(Json.write(r)).append('\n');
                Path tmp = dir.resolve(partName + ".tmp");
                Files.writeString(tmp, sb.toString());
                Files.move(tmp, dir.resolve(partName), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                files.add(Map.of("path", relDir + "/" + partName, "rows", (long) pe.getValue().size()));
            }

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("seq", seq);
            entry.put("txn_from", txnFrom);
            entry.put("txn_to", txnTo);
            entry.put("rows", (long) rows.size());
            entry.put("files", files);
            entry.put("engine", Map.of("name", "chronodim", "format", "chronodim-open-v1"));
            entry.put("load_ids", loadIds.stream().distinct().toList());
            entry.put("created_ms", System.currentTimeMillis());
            Path logTmp = t.location.resolve(PublishedContract.LOG_DIR).resolve(String.format("%020d.json.tmp", seq));
            Files.writeString(logTmp, Json.writePretty(entry));
            Files.move(logTmp, t.location.resolve(PublishedContract.LOG_DIR).resolve(String.format("%020d.json", seq)),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            t.logSeq = seq;
            t.publishedTxn = txnTo;
        } catch (IOException e) {
            throw new UncheckedIOException("publish failed for " + t.location, e);
        }
    }

    /** Renders one version record for publishing; null = suppressed in this style (Databricks tombstones). */
    static Map<String, Object> renderRow(TableRuntime rt, Codecs.DataKey k, byte[] value, PublishedContract.Style style) {
        Codecs.DecodedValue v = Codecs.decodeValue(value, sv -> rt.config().schemaAt(sv));
        if (!style.includeOps() && v.op() == Op.DELETE.code) {
            // Databricks shape: a delete is the predecessor's closed __END_AT with
            // no successor row — the tombstone itself has no representation.
            return null;
        }
        TableSchema written = rt.config().schemaAt(v.schemaVersion());
        Map<String, Object> out = new LinkedHashMap<>();
        for (Column col : rt.config().currentSchema().columns()) {
            int idx = written.indexOf(col.name());
            Object val = idx >= 0 ? v.payload()[idx] : null;
            out.put(col.name(), val == null ? null : col.type().render(val));
        }
        out.put(style.startCol(), io.chronodim.api.ColumnType.formatTimestampMicros(k.validFrom()));
        out.put(style.endCol(), v.validTo() == io.chronodim.api.Version.OPEN
                ? null : io.chronodim.api.ColumnType.formatTimestampMicros(v.validTo()));
        out.put(PublishedContract.TX_TIME, io.chronodim.api.ColumnType.formatTimestampMicros(v.txTime()));
        out.put(PublishedContract.TXN_ID, v.txnId());
        if (style.includeOps()) {
            out.put(PublishedContract.OP, Op.fromCode(v.op()).name());
            out.put(PublishedContract.SCHEMA_VERSION, (long) v.schemaVersion());
        }
        return out;
    }

    private static String loadIdOf(String manifestJson) {
        if (manifestJson == null || manifestJson.isBlank() || manifestJson.equals("{}")) return null;
        Object v = Json.parseObject(manifestJson).get("load_id");
        return v == null ? null : String.valueOf(v);
    }

    @Override
    public boolean allowWalPrune(String segmentName, long maxTxnInSegment) {
        // The publisher tails the WAL: a segment may only vanish once every
        // published table's watermark has passed its highest transaction.
        synchronized (this) {
            for (TableTarget t : targets.values()) {
                if (t.publishedTxn < maxTxnInSegment) return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, Object> stats() {
        if (engine == null || targets.isEmpty()) return Map.of();
        long minWatermark = Long.MAX_VALUE;
        for (TableTarget t : targets.values()) minWatermark = Math.min(minWatermark, t.publishedTxn);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("publish_lag_txns", Math.max(0, engine.lastCommittedTxn() - minWatermark));
        m.put("published_txn", minWatermark);
        m.put("last_publish_age_ms", lastPublishAtMillis == 0 ? -1 : System.currentTimeMillis() - lastPublishAtMillis);
        return m;
    }

    @Override
    public void close() {
        running = false;
        if (worker != null) {
            synchronized (signal) {
                signal.notifyAll();
            }
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            publishCycle(); // final drain so a clean shutdown leaves zero lag
        }
    }

    /** Interface reserved for the Delta Kernel / Parquet part writer (drop-in, R-PUB contract unchanged). */
    interface PartWriter {
        void write(Path target, List<Map<String, Object>> rows) throws IOException;
    }
}
