package io.chronodim.cli;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.ChronoDimException;
import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.QuarantineEntry;
import io.chronodim.api.TableConfig;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableConfigIO;
import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.util.Csv;
import io.chronodim.core.util.Json;
import io.chronodim.durability.ObjectStore;
import io.chronodim.durability.SnapshotManager;
import io.chronodim.export.Finalizer;
import io.chronodim.export.PublisherPlugin;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * {@code chronodim} — embedded bitemporal SCD2 storage engine CLI (R-CLI).
 * Every command supports {@code --data-dir} and {@code --json}; failures exit
 * non-zero (1 = engine error, 2 = usage/config/validation).
 */
@Command(name = "chronodim",
        mixinStandardHelpOptions = true,
        version = "chronodim 1.0.0",
        description = "Embedded bitemporal SCD Type 2 storage engine.",
        subcommands = {
                Main.Init.class, Main.Table.class, Main.Load.class, Main.Apply.class,
                Main.Get.class, Main.AsOf.class, Main.History.class, Main.Scan.class,
                Main.ExportStatus.class, Main.FinalizeCmd.class,
                Main.Snapshot.class, Main.WalPrune.class, Main.Restore.class, Main.Verify.class,
                Main.Manifest.class, Main.Quarantine.class, Main.Stats.class, Main.Bench.class,
        })
public final class Main {

    public static void main(String[] args) {
        CommandLine cl = new CommandLine(new Main());
        cl.setExecutionExceptionHandler((ex, cmd, parse) -> {
            String msg = ex.getMessage() != null ? ex.getMessage() : ex.toString();
            cmd.getErr().println(cmd.getColorScheme().errorText("error: " + msg));
            if (System.getenv("CHRONODIM_DEBUG") != null) ex.printStackTrace(cmd.getErr());
            return (ex instanceof ValidationException || ex instanceof ConfigException) ? 2 : 1;
        });
        System.exit(cl.execute(args));
    }

    /** Options shared by every engine-opening command. */
    static class EngineOpts {
        @Option(names = {"-d", "--data-dir"}, defaultValue = "${CHRONODIM_DATA:-./chronodim-data}",
                description = "Database directory (env CHRONODIM_DATA; default ./chronodim-data)")
        Path dataDir;

        @Option(names = "--json", description = "Machine-readable JSON output")
        boolean json;

        @Option(names = "--storage", defaultValue = "rocksdb", description = "Hot store backend: rocksdb (default) or lsm (pure Java)")
        String storage;

        @Option(names = "--no-publish", description = "Disable the cold-tier publisher for this process")
        boolean noPublish;

        @Option(names = "--object-store", description = "Object store URI for WAL shipping/snapshots (file:/path or mounted bucket)")
        String objectStore;

        @Option(names = "--no-fsync", hidden = true, description = "Testing only: forfeit durability")
        boolean noFsync;

        EngineOptions options() {
            return EngineOptions.builder()
                    .storageBackend(EngineOptions.StorageBackend.valueOf(storage.toUpperCase(java.util.Locale.ROOT)))
                    .publishEnabled(!noPublish)
                    .objectStoreUri(objectStore)
                    .fsync(!noFsync)
                    .build();
        }

        Engine open() {
            return ChronoDim.open(dataDir, options());
        }

        void out(Object humanOrMap) {
            PrintWriter w = new PrintWriter(System.out, true);
            if (json) {
                w.println(Json.write(humanOrMap));
            } else if (humanOrMap instanceof Map<?, ?> m) {
                w.println(Json.writePretty(m));
            } else {
                w.println(humanOrMap);
            }
        }
    }

    static Map<String, Object> renderVersion(Engine e, String table, Version v) {
        TableConfig cfg = e.describeTable(table);
        Map<String, Object> row = new LinkedHashMap<>();
        for (var col : cfg.currentSchema().columns()) {
            row.put(col.name(), col.type().render(v.row().get(col.name())));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("row", row);
        out.put("_valid_from", ColumnType.formatTimestampMicros(v.validFrom()));
        out.put("_valid_to", v.validToIsOpen() ? null : ColumnType.formatTimestampMicros(v.validTo()));
        out.put("_tx_time", ColumnType.formatTimestampMicros(v.txTime()));
        out.put("_txn_id", v.txnId());
        out.put("_op", v.op().name());
        out.put("_schema_version", (long) v.schemaVersion());
        out.put("_is_current", v.current());
        return out;
    }

    static Map<String, Object> parseKey(List<String> kvs) {
        Map<String, Object> key = new LinkedHashMap<>();
        for (String kv : kvs) {
            int i = kv.indexOf('=');
            if (i < 1) throw new ValidationException("business key must be column=value, got '" + kv + "'");
            key.put(kv.substring(0, i), kv.substring(i + 1));
        }
        return key;
    }

    static Map<String, Object> manifestMap(AuditManifest m) {
        return Json.parseObject(io.chronodim.core.engine.ManifestCodec.toJson(m));
    }

    // ------------------------------------------------------------------------------

    @Command(name = "init", description = "Create/initialise a database directory.")
    static class Init implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                opts.out(Map.of("initialized", opts.dataDir.toString(), "last_committed_txn", e.lastCommittedTxn()));
            }
            return 0;
        }
    }

    @Command(name = "table", description = "Table management.", subcommands = {
            Table.Create.class, Table.Alter.class, Table.ListCmd.class, Table.Describe.class})
    static class Table {
        @Command(name = "create", description = "Create a table from a YAML (or JSON) config file.")
        static class Create implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Option(names = {"-f", "--file"}, required = true, description = "Table config YAML")
            Path file;

            @Override
            public Integer call() throws Exception {
                TableConfig cfg = TableConfigIO.fromYaml(Files.readString(file));
                try (Engine e = opts.open()) {
                    e.createTable(cfg);
                    opts.out(Map.of("created", cfg.table()));
                }
                return 0;
            }
        }

        @Command(name = "alter", description = "Apply a compatible config change (add nullable columns, policies, gates).")
        static class Alter implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Option(names = {"-f", "--file"}, required = true)
            Path file;

            @Override
            public Integer call() throws Exception {
                TableConfig cfg = TableConfigIO.fromYaml(Files.readString(file));
                try (Engine e = opts.open()) {
                    e.alterTable(cfg);
                    opts.out(Map.of("altered", cfg.table(),
                            "schema_version", (long) e.describeTable(cfg.table()).currentSchema().schemaVersion()));
                }
                return 0;
            }
        }

        @Command(name = "list", description = "List tables.")
        static class ListCmd implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    opts.out(Map.of("tables", e.listTables()));
                }
                return 0;
            }
        }

        @Command(name = "describe", description = "Show a table's stored configuration.")
        static class Describe implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Parameters(index = "0")
            String table;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    opts.out(((EngineImpl) e).catalog().describe(table));
                }
                return 0;
            }
        }
    }

    /** Shared input-shaping: envelope resolution, SCD2-interval import, effective_at defaults. */
    static class IngestOpts {
        @Option(names = "--load-id", description = "Idempotency key (or 'load_id' inside a JSON envelope)")
        String loadId;
        @Option(names = "--format", description = "json | jsonl | csv (default: by extension)")
        String format;
        @Option(names = "--scd2-start-column", defaultValue = "__START_AT",
                description = "Interval-import: start column in the input (default __START_AT)")
        String scd2Start;
        @Option(names = "--scd2-end-column", defaultValue = "__END_AT",
                description = "Interval-import: end column in the input (default __END_AT; null/absent = active)")
        String scd2End;

        String resolveLoadId(String fromEnvelope) {
            if (loadId != null && fromEnvelope != null && !loadId.equals(fromEnvelope)) {
                throw new ValidationException("--load-id '" + loadId + "' conflicts with envelope load_id '" + fromEnvelope + "'");
            }
            String resolved = loadId != null ? loadId : fromEnvelope;
            if (resolved == null) {
                throw new ValidationException("a load id is required: pass --load-id or put load_id in the JSON envelope");
            }
            return resolved;
        }

        /** Interval-import (rows already carrying start/end) + envelope effective_at defaults. */
        List<InputRow> shapeRows(Engine e, String table, List<InputRow> rows, String effectiveAt) {
            TableConfig cfg = e.describeTable(table);
            if (io.chronodim.core.scd2.Scd2Import.looksLikeScd2(rows, scd2Start)) {
                rows = io.chronodim.core.scd2.Scd2Import.toInputRows(
                        ((EngineImpl) e).catalog().get(table), rows, scd2Start, scd2End);
            }
            if (effectiveAt == null) return rows;
            long micros = ColumnType.parseTimestampMicros(effectiveAt);
            List<InputRow> out = new ArrayList<>(rows.size());
            for (InputRow r : rows) {
                if (r.validFromMicrosOverride() != null) {
                    out.add(r); // interval import already decided
                } else if (cfg.validTimeMode() == TableConfig.ValidTimeMode.SOURCE_COLUMN
                        && r.values().get(cfg.validTimeColumn()) != null) {
                    out.add(r); // the record carries its own effective time
                } else {
                    out.add(r.withValidFrom(micros));
                }
            }
            return out;
        }
    }

    @Command(name = "load", description = "Bulk backfill an empty table from a file (sorted-ingest fast path).")
    static class Load implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @CommandLine.Mixin
        IngestOpts ingest;
        @Parameters(index = "0", description = "Input file (.json/.jsonl/.csv)")
        Path file;
        @Option(names = {"-t", "--table"}, description = "Target table (or 'table' inside a JSON envelope)")
        String table;

        @Override
        public Integer call() {
            RowFileReader.ParsedInput in = RowFileReader.read(file, ingest.format, table);
            if (in.byTable().size() != 1) throw new ValidationException("load supports a single table");
            String t = in.byTable().keySet().iterator().next();
            try (Engine e = opts.open()) {
                List<InputRow> rows = ingest.shapeRows(e, t, in.byTable().get(t), in.effectiveAt());
                AuditManifest m = e.backfill(ingest.resolveLoadId(in.loadId()), t, rows);
                opts.out(manifestMap(m));
            }
            return 0;
        }
    }

    @Command(name = "apply", description = "Apply a CDC/adjustment batch from a file or stdin, atomically and idempotently.")
    static class Apply implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @CommandLine.Mixin
        IngestOpts ingest;
        @Parameters(index = "0", arity = "0..1", description = "Input file; omit with --stdin")
        Path file;
        @Option(names = {"-t", "--table"}, description = "Target table (not needed for envelopes or object-of-tables input)")
        String table;
        @Option(names = "--stdin", description = "Read rows from stdin (default format jsonl)")
        boolean stdin;
        @Option(names = "--full-snapshot", description = "Rows are the complete population: active keys absent from the input are soft-deleted")
        boolean fullSnapshot;

        @Override
        public Integer call() {
            RowFileReader.ParsedInput in = stdin
                    ? RowFileReader.readStdin(ingest.format, table)
                    : RowFileReader.read(java.util.Objects.requireNonNull(file, "input file or --stdin required"), ingest.format, table);
            try (Engine e = opts.open()) {
                List<ApplyBatch.TableBatch> batches = new ArrayList<>();
                in.byTable().forEach((t, rows) ->
                        batches.add(new ApplyBatch.TableBatch(t, ingest.shapeRows(e, t, rows, in.effectiveAt()), fullSnapshot)));
                AuditManifest m = e.apply(new ApplyBatch(ingest.resolveLoadId(in.loadId()), batches, in.metadata()));
                opts.out(manifestMap(m));
            }
            return 0;
        }
    }

    @Command(name = "get", description = "Latest current version of an entity.")
    static class Get implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = {"-t", "--table"}, required = true)
        String table;
        @Parameters(description = "Business key: column=value ...")
        List<String> key;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                Optional<Version> v = e.getCurrent(table, parseKey(key));
                if (v.isEmpty()) {
                    opts.out(Map.of("found", false));
                    return 3;
                }
                opts.out(renderVersion(e, table, v.get()));
            }
            return 0;
        }
    }

    @Command(name = "asof", description = "Point-in-time read; add --tx-time for a bitemporal read (R-READ-5).")
    static class AsOf implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = {"-t", "--table"}, required = true)
        String table;
        @Option(names = "--valid-time", required = true, description = "ISO-8601 or epoch micros")
        String validTime;
        @Option(names = "--tx-time", description = "What did the engine believe at this transaction time?")
        String txTime;
        @Parameters(description = "Business key: column=value ...")
        List<String> key;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                long vt = ColumnType.parseTimestampMicros(validTime);
                Optional<Version> v = txTime == null
                        ? e.getAsOf(table, parseKey(key), vt)
                        : e.getAsOf(table, parseKey(key), vt, ColumnType.parseTimestampMicros(txTime));
                if (v.isEmpty()) {
                    opts.out(Map.of("found", false));
                    return 3;
                }
                opts.out(renderVersion(e, table, v.get()));
            }
            return 0;
        }
    }

    @Command(name = "history", description = "Full bitemporal version history of an entity, newest first.")
    static class History implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = {"-t", "--table"}, required = true)
        String table;
        @Parameters(description = "Business key: column=value ...")
        List<String> key;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                List<Object> out = new ArrayList<>();
                for (Version v : e.getHistory(table, parseKey(key))) {
                    out.add(renderVersion(e, table, v));
                }
                opts.out(Map.of("versions", out));
            }
            return 0;
        }
    }

    @Command(name = "scan", description = "Stream the whole table (current or as-of) to stdout or a file.")
    static class Scan implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = {"-t", "--table"}, required = true)
        String table;
        @Option(names = "--as-of", description = "Valid time (default: current state)")
        String asOf;
        @Option(names = "--format", defaultValue = "jsonl", description = "jsonl | csv")
        String format;
        @Option(names = {"-o", "--output"}, description = "Output file (default stdout)")
        Path output;

        @Override
        public Integer call() throws Exception {
            try (Engine e = opts.open()) {
                TableConfig cfg = e.describeTable(table);
                try (VersionCursor c = asOf == null
                        ? e.scanCurrent(table)
                        : e.scanAsOf(table, ColumnType.parseTimestampMicros(asOf));
                     var w = output == null
                             ? new PrintWriter(System.out)
                             : new PrintWriter(Files.newBufferedWriter(output))) {
                    long n = 0;
                    if (format.equals("csv")) {
                        List<String> header = new ArrayList<>();
                        cfg.currentSchema().columns().forEach(col -> header.add(col.name()));
                        header.add("_valid_from");
                        w.println(String.join(",", header));
                        while (c.hasNext()) {
                            Version v = c.next();
                            List<String> cells = new ArrayList<>();
                            for (var col : cfg.currentSchema().columns()) {
                                Object r = col.type().render(v.row().get(col.name()));
                                cells.add(Csv.escape(r == null ? "" : String.valueOf(r)));
                            }
                            cells.add(ColumnType.formatTimestampMicros(v.validFrom()));
                            w.println(String.join(",", cells));
                            n++;
                        }
                    } else {
                        while (c.hasNext()) {
                            w.println(Json.write(renderVersion(e, table, c.next())));
                            n++;
                        }
                    }
                    w.flush();
                    if (output != null) System.err.println(n + " rows -> " + output);
                }
            }
            return 0;
        }
    }

    @Command(name = "export-status", description = "Publisher watermarks and lag per published table.")
    static class ExportStatus implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                Map<String, Object> stats = e.stats();
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("last_committed_txn", e.lastCommittedTxn());
                for (String k : List.of("published_txn", "publish_lag_txns", "last_publish_age_ms")) {
                    if (stats.containsKey(k)) out.put(k, stats.get(k));
                }
                opts.out(out);
            }
            return 0;
        }
    }

    @Command(name = "finalize", description = "Consolidate published parts into finalized SCD2 shape (R-PUB-4).")
    static class FinalizeCmd implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = {"-t", "--table"}, required = true)
        String table;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                TableConfig cfg = e.describeTable(table);
                if (!cfg.publish().enabled()) throw new ConfigException("table '" + table + "' has publishing disabled");
                Path location = PublisherPlugin.resolveLocation(cfg.publish().location());
                opts.out(Finalizer.run(location, cfg.businessKey(), cfg.publish().partitionBy(),
                        io.chronodim.export.PublishedContract.Style.forConfig(cfg.publish())));
            }
            return 0;
        }
    }

    @Command(name = "snapshot", description = "Checkpoint the hot store; upload to the object store when configured (R-DUR-2).")
    static class Snapshot implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = "--prune-wal", description = "After the checkpoint, delete local WAL segments that are durable, published and shipped")
        boolean pruneWal;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                Map<String, Object> snap = e.snapshot();
                if (opts.objectStore != null) {
                    ObjectStore store = ObjectStore.open(opts.objectStore);
                    Map<String, Object> up = SnapshotManager.upload(store,
                            Path.of(String.valueOf(snap.get("path"))), ((Number) snap.get("txn_id")).longValue());
                    snap = new LinkedHashMap<>(snap);
                    snap.put("uploaded", up.get("key"));
                    snap.put("zip_bytes", up.get("zip_bytes"));
                }
                if (pruneWal) {
                    snap = new LinkedHashMap<>(snap);
                    snap.put("wal_prune", e.pruneWal());
                }
                opts.out(snap);
            }
            return 0;
        }
    }

    @Command(name = "wal-prune", description = "Delete local WAL segments no longer needed (durable + published + shipped). Shipped copies in the object store are untouched.")
    static class WalPrune implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                opts.out(e.pruneWal());
            }
            return 0;
        }
    }

    @Command(name = "restore", description = "Restore a database from the object store (R-DUR-3). Does not require a running engine.")
    static class Restore implements Callable<Integer> {
        @Option(names = "--from", required = true, description = "Object store URI")
        String from;
        @Option(names = "--target-dir", required = true)
        Path targetDir;
        @Option(names = "--as-of-txn", description = "Stop replay at this transaction id")
        Long asOfTxn;
        @Option(names = "--json")
        boolean json;

        @Override
        public Integer call() {
            Map<String, Object> report = SnapshotManager.restore(ObjectStore.open(from), targetDir, asOfTxn);
            // Open once so recovery replays the WAL and the state fingerprint is real.
            try (Engine e = ChronoDim.open(targetDir, EngineOptions.builder().publishEnabled(false).build())) {
                Map<String, Object> verify = e.verify();
                report = new LinkedHashMap<>(report);
                report.put("recovered_txn", e.lastCommittedTxn());
                report.put("state_fingerprint", verify.get("state_fingerprint"));
            }
            System.out.println(json ? Json.write(report) : Json.writePretty(report));
            return 0;
        }
    }

    @Command(name = "verify", description = "Full-scan state fingerprint + per-table counts (R-DUR-5).")
    static class Verify implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                opts.out(e.verify());
            }
            return 0;
        }
    }

    @Command(name = "manifest", description = "Audit manifests (R-APPLY-7).", subcommands = {
            Manifest.Show.class, Manifest.ListCmd.class})
    static class Manifest {
        @Command(name = "show", description = "Show the manifest for a load id.")
        static class Show implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Parameters(index = "0")
            String loadId;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    Optional<AuditManifest> m = e.manifest(loadId);
                    if (m.isEmpty()) {
                        opts.out(Map.of("found", false));
                        return 3;
                    }
                    opts.out(manifestMap(m.get()));
                }
                return 0;
            }
        }

        @Command(name = "list", description = "List recent manifests, newest first.")
        static class ListCmd implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Option(names = {"-t", "--table"}, description = "Filter by table")
            String table;
            @Option(names = {"-n", "--limit"}, defaultValue = "20")
            int limit;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    List<Object> out = new ArrayList<>();
                    for (AuditManifest m : e.listManifests(table, limit)) out.add(manifestMap(m));
                    opts.out(Map.of("manifests", out));
                }
                return 0;
            }
        }
    }

    @Command(name = "quarantine", description = "Inspect and re-apply quarantined rows.", subcommands = {
            Quarantine.ListCmd.class, Quarantine.Reapply.class})
    static class Quarantine {
        @Command(name = "list")
        static class ListCmd implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Option(names = {"-t", "--table"}, required = true)
            String table;
            @Option(names = {"-n", "--limit"}, defaultValue = "100")
            int limit;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    List<Object> out = new ArrayList<>();
                    for (QuarantineEntry q : e.quarantineList(table, limit)) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("id", q.id());
                        m.put("reason", q.reason());
                        m.put("load_id", q.loadId());
                        m.put("at", ColumnType.formatTimestampMicros(q.quarantinedAtMicros()));
                        m.put("delete", q.delete());
                        m.put("values", q.values());
                        out.add(m);
                    }
                    opts.out(Map.of("quarantined", out));
                }
                return 0;
            }
        }

        @Command(name = "reapply", description = "Re-apply quarantined rows under a new load id.")
        static class Reapply implements Callable<Integer> {
            @CommandLine.Mixin
            EngineOpts opts;
            @Option(names = {"-t", "--table"}, required = true)
            String table;
            @Option(names = "--load-id", required = true)
            String loadId;
            @Option(names = "--ids", split = ",", description = "Specific entry ids (default: all)")
            List<Long> ids;

            @Override
            public Integer call() {
                try (Engine e = opts.open()) {
                    opts.out(manifestMap(e.quarantineReapply(table, ids == null ? List.of() : ids, loadId)));
                }
                return 0;
            }
        }
    }

    @Command(name = "stats", description = "Engine statistics (R-OBS).")
    static class Stats implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;

        @Override
        public Integer call() {
            try (Engine e = opts.open()) {
                opts.out(e.stats());
            }
            return 0;
        }
    }

    @Command(name = "bench", description = "Quick standard benchmark (bulk load + CDC apply + point reads) on this machine.")
    static class Bench implements Callable<Integer> {
        @CommandLine.Mixin
        EngineOpts opts;
        @Option(names = "--rows", defaultValue = "1000000", description = "Bulk rows (default 1M; use 50000000 for the §10 P1 run)")
        long rows;
        @Option(names = "--changes", defaultValue = "200000", description = "CDC changes (default 200k)")
        long changes;

        @Override
        public Integer call() {
            if (!(opts.dataDir.toFile().exists()) || java.util.Objects.requireNonNull(opts.dataDir.toFile().list()).length == 0) {
                opts.out(BenchCore.run(opts.dataDir, opts.options(), rows, changes, opts.json));
                return 0;
            }
            throw new ValidationException("bench needs an empty --data-dir (it creates and fills a database)");
        }
    }
}
