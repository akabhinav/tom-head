package io.chronodim.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDimException;
import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.Engine;
import io.chronodim.api.InputRow;
import io.chronodim.api.QuarantineEntry;
import io.chronodim.api.TableConfig;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableConfigIO;
import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.engine.ManifestCodec;
import io.chronodim.core.scd2.Scd2Import;
import io.chronodim.core.util.Json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * The built-in admin console (R-UI): a tiny HTTP layer over the embedded engine
 * plus one static HTML page. Deliberately framework-free — the server is the
 * JDK's {@code com.sun.net.httpserver}, the page is hand-written HTML/CSS/JS.
 *
 * <p>Concurrency: many browser tabs / users may hit this at once. That is the
 * supported pattern — ONE process owns the data directory, and {@link Engine}
 * is thread-safe (reads are snapshot-isolated, {@code apply} serializes through
 * the fair commit mutex with shared group commits).
 *
 * <p>Binds to 127.0.0.1 by default: the console has no authentication, so
 * exposing it beyond the machine is an explicit operator decision
 * ({@code --host 0.0.0.0}).
 */
final class UiServer {

    private final Engine engine;
    private final HttpServer http;

    private UiServer(Engine engine, HttpServer http) {
        this.engine = engine;
        this.http = http;
    }

    static UiServer start(Engine engine, String host, int port) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(host, port), 0);
        UiServer s = new UiServer(engine, http);
        http.createContext("/", s::route);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.start();
        return s;
    }

    int port() {
        return http.getAddress().getPort();
    }

    void stop() {
        http.stop(0);
    }

    // ---- routing ---------------------------------------------------------------

    private void route(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String method = ex.getRequestMethod();
        try {
            if (!path.startsWith("/api/")) {
                servePage(ex, path);
                return;
            }
            String[] p = path.substring("/api/".length()).split("/", -1); // e.g. tables/customer/rows
            Map<String, String> q = query(ex);
            Object result = switch (p[0]) {
                case "tables" -> switch (p.length) {
                    case 1 -> method.equals("POST") ? createTable(body(ex)) : listTables();
                    case 2 -> describe(p[1]);
                    case 3 -> switch (p[2]) {
                        case "rows" -> rows(p[1], q);
                        case "history" -> history(p[1], q);
                        case "apply" -> requirePost(method, () -> apply(p[1], q, body(ex)));
                        default -> throw new NotFound();
                    };
                    default -> throw new NotFound();
                };
                case "manifests" -> manifests(q);
                case "quarantine" -> p.length > 1 && p[1].equals("reapply")
                        ? requirePost(method, () -> reapply(body(ex)))
                        : quarantine(q);
                case "stats" -> stats();
                case "verify" -> engine.verify();
                default -> throw new NotFound();
            };
            send(ex, method.equals("POST") && p[0].equals("tables") && p.length == 1 ? 201 : 200, result);
        } catch (NotFound e) {
            send(ex, 404, Map.of("error", "not found: " + method + " " + path));
        } catch (ValidationException | ConfigException | IllegalArgumentException
                 | java.time.DateTimeException e) {
            send(ex, 400, Map.of("error", String.valueOf(e.getMessage())));
        } catch (ChronoDimException e) {
            send(ex, 409, Map.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            send(ex, 500, Map.of("error", e.toString()));
        }
    }

    private static final class NotFound extends RuntimeException {}

    private interface Handler {
        Object run() throws IOException;
    }

    private static Object requirePost(String method, Handler h) throws IOException {
        if (!method.equals("POST")) throw new ValidationException("this endpoint requires POST");
        return h.run();
    }

    // ---- catalog ----------------------------------------------------------------

    private Object listTables() {
        List<Object> out = new ArrayList<>();
        for (String t : engine.listTables()) out.add(describe(t));
        return Map.of("tables", out);
    }

    private Map<String, Object> describe(String table) {
        TableConfig c = engine.describeTable(table);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("table", c.table());
        m.put("business_key", c.businessKey());
        List<Object> cols = new ArrayList<>();
        for (Column col : c.currentSchema().columns()) {
            cols.add(Map.of("name", col.name(), "type", col.typeDeclaration(),
                    "key", c.businessKey().contains(col.name())));
        }
        m.put("columns", cols);
        m.put("schema_version", (long) c.currentSchema().schemaVersion());
        m.put("valid_time_mode", c.validTimeMode().name());
        if (c.validTimeColumn() != null) m.put("valid_time_column", c.validTimeColumn());
        m.put("late_arrival", c.lateArrivalPolicy().name());
        m.put("delete_mode", c.deleteMode().name());
        m.put("on_row_failure", c.batchFailurePolicy().name());
        m.put("tracked_columns", c.effectiveTrackedColumns());
        m.put("ignored_columns", c.ignoredColumns());
        List<Object> gates = new ArrayList<>();
        for (var g : c.qualityGates()) gates.add(Map.of("column", g.column(), "rule", g.rule()));
        m.put("quality_gates", gates);
        Map<String, Object> pub = new LinkedHashMap<>();
        pub.put("enabled", c.publish().enabled());
        if (c.publish().enabled()) {
            pub.put("location", c.publish().location());
            pub.put("partition_by", c.publish().partitionBy());
            pub.put("start_column", c.publish().effectiveStartColumn());
            pub.put("end_column", c.publish().effectiveEndColumn());
        }
        m.put("publish", pub);
        return m;
    }

    /** Body is a table config as YAML or JSON (YAML is a superset, one parser serves both). */
    private Object createTable(String body) {
        TableConfig cfg = TableConfigIO.fromYaml(body);
        engine.createTable(cfg);
        return describe(cfg.table());
    }

    // ---- reads --------------------------------------------------------------------

    /**
     * Paged scan of current (or as-of) state. {@code q} filters by substring over
     * the rendered row values — a browse aid, not an index; the scan still walks
     * the table in key order and stops after one page plus one row (has_more).
     */
    private Object rows(String table, Map<String, String> q) {
        TableConfig cfg = engine.describeTable(table);
        int limit = Math.min(intParam(q, "limit", 50), 1000);
        int offset = intParam(q, "offset", 0);
        String needle = q.get("q") == null ? null : q.get("q").toLowerCase(java.util.Locale.ROOT);
        String asOf = q.get("as_of");

        List<Object> rows = new ArrayList<>();
        long matched = 0;
        boolean hasMore = false;
        try (VersionCursor c = asOf == null || asOf.isBlank()
                ? engine.scanCurrent(table)
                : engine.scanAsOf(table, ColumnType.parseTimestampMicros(asOf))) {
            while (c.hasNext()) {
                Version v = c.next();
                Map<String, Object> r = renderVersion(cfg, v);
                if (needle != null && !Json.write(r).toLowerCase(java.util.Locale.ROOT).contains(needle)) continue;
                matched++;
                if (matched <= offset) continue;
                if (rows.size() == limit) {
                    hasMore = true;
                    break;
                }
                rows.add(r);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rows", rows);
        out.put("offset", (long) offset);
        out.put("limit", (long) limit);
        out.put("has_more", hasMore);
        return out;
    }

    /** Business key comes from query params (?id=C1&region=EU); engine coerces the strings. */
    private Object history(String table, Map<String, String> q) {
        TableConfig cfg = engine.describeTable(table);
        Map<String, Object> key = new LinkedHashMap<>();
        for (String k : cfg.businessKey()) {
            if (q.get(k) == null) throw new ValidationException("missing business key column '" + k + "'");
            key.put(k, q.get(k));
        }
        String asOf = q.get("as_of");
        if (asOf != null && !asOf.isBlank()) {
            Optional<Version> v = engine.getAsOf(table, key, ColumnType.parseTimestampMicros(asOf));
            return Map.of("versions", v.map(x -> List.<Object>of(renderVersion(cfg, x))).orElse(List.of()));
        }
        List<Object> out = new ArrayList<>();
        for (Version v : engine.getHistory(table, key)) out.add(renderVersion(cfg, v));
        return Map.of("versions", out);
    }

    private static Map<String, Object> renderVersion(TableConfig cfg, Version v) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Column col : cfg.currentSchema().columns()) {
            row.put(col.name(), col.type().render(v.row().get(col.name())));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("row", row);
        out.put("_valid_from", ColumnType.formatTimestampMicros(v.validFrom()));
        out.put("_valid_to", v.validToIsOpen() ? null : ColumnType.formatTimestampMicros(v.validTo()));
        out.put("_tx_time", ColumnType.formatTimestampMicros(v.txTime()));
        out.put("_txn_id", v.txnId());
        out.put("_op", v.op().name());
        out.put("_is_current", v.current());
        return out;
    }

    // ---- writes -------------------------------------------------------------------

    /**
     * Accepts exactly what the CLI accepts: a metadata envelope, a bare JSON array,
     * JSONL or CSV (?format=). load_id/effective_at/full_snapshot may come from the
     * envelope or query params. Interval inputs (__START_AT/__END_AT) auto-detect.
     */
    private Object apply(String table, Map<String, String> q, String body) {
        RowFileReader.ParsedInput in = RowFileReader.readString(body, q.get("format"), table);
        String loadId = in.loadId() != null ? in.loadId() : q.get("load_id");
        if (loadId == null || loadId.isBlank()) {
            throw new ValidationException("a load_id is required (envelope field or ?load_id=)");
        }
        boolean fullSnapshot = Boolean.parseBoolean(q.getOrDefault("full_snapshot", "false"));
        String effectiveAt = in.effectiveAt() != null ? in.effectiveAt() : q.get("effective_at");

        List<ApplyBatch.TableBatch> batches = new ArrayList<>();
        in.byTable().forEach((t, rows) ->
                batches.add(new ApplyBatch.TableBatch(t, shape(t, rows, effectiveAt), fullSnapshot)));
        AuditManifest m = engine.apply(new ApplyBatch(loadId, batches, in.metadata()));
        Map<String, Object> out = Json.parseObject(ManifestCodec.toJson(m));
        out.put("already_applied", m.alreadyApplied());
        return out;
    }

    /** Same shaping as the CLI: SCD2 interval auto-detect, then effective_at defaults. */
    private List<InputRow> shape(String table, List<InputRow> rows, String effectiveAt) {
        TableConfig cfg = engine.describeTable(table);
        if (Scd2Import.looksLikeScd2(rows, "__START_AT")) {
            rows = Scd2Import.toInputRows(((EngineImpl) engine).catalog().get(table), rows, "__START_AT", "__END_AT");
        }
        if (effectiveAt == null || effectiveAt.isBlank()) return rows;
        long micros = ColumnType.parseTimestampMicros(effectiveAt);
        List<InputRow> out = new ArrayList<>(rows.size());
        for (InputRow r : rows) {
            boolean carriesOwnTime = r.validFromMicrosOverride() != null
                    || (cfg.validTimeMode() == TableConfig.ValidTimeMode.SOURCE_COLUMN
                        && r.values().get(cfg.validTimeColumn()) != null);
            out.add(carriesOwnTime ? r : r.withValidFrom(micros));
        }
        return out;
    }

    // ---- audit / quarantine / ops ---------------------------------------------------

    private Object manifests(Map<String, String> q) {
        List<Object> out = new ArrayList<>();
        for (AuditManifest m : engine.listManifests(q.get("table"), intParam(q, "limit", 50))) {
            out.add(Json.parseObject(ManifestCodec.toJson(m)));
        }
        return Map.of("manifests", out);
    }

    private Object quarantine(Map<String, String> q) {
        String table = q.get("table");
        if (table == null) throw new ValidationException("?table= is required");
        List<Object> out = new ArrayList<>();
        for (QuarantineEntry e : engine.quarantineList(table, intParam(q, "limit", 200))) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", e.id());
            m.put("reason", e.reason());
            m.put("load_id", e.loadId());
            m.put("at", ColumnType.formatTimestampMicros(e.quarantinedAtMicros()));
            m.put("delete", e.delete());
            m.put("values", e.values());
            out.add(m);
        }
        return Map.of("quarantined", out);
    }

    @SuppressWarnings("unchecked")
    private Object reapply(String body) {
        Map<String, Object> req = Json.parseObject(body);
        String table = (String) req.get("table");
        String loadId = (String) req.get("load_id");
        if (table == null || loadId == null) throw new ValidationException("table and load_id are required");
        List<Long> ids = new ArrayList<>();
        if (req.get("ids") instanceof List<?> l) for (Object o : l) ids.add(((Number) o).longValue());
        return Json.parseObject(ManifestCodec.toJson(engine.quarantineReapply(table, ids, loadId)));
    }

    private Object stats() {
        Map<String, Object> out = new LinkedHashMap<>(engine.stats());
        out.put("last_committed_txn", engine.lastCommittedTxn());
        out.put("tables", engine.listTables());
        return out;
    }

    // ---- http plumbing ---------------------------------------------------------------

    private void servePage(HttpExchange ex, String path) throws IOException {
        if (!path.equals("/") && !path.equals("/index.html")) {
            send(ex, 404, Map.of("error", "not found"));
            return;
        }
        byte[] page;
        try (InputStream in = UiServer.class.getResourceAsStream("/ui/index.html")) {
            if (in == null) throw new IOException("ui/index.html missing from jar");
            page = in.readAllBytes();
        }
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        ex.sendResponseHeaders(200, page.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(page);
        }
    }

    private static void send(HttpExchange ex, int status, Object json) throws IOException {
        byte[] b = Json.write(json).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(b);
        }
    }

    private static String body(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> out = new LinkedHashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) return out;
        for (String pair : raw.split("&")) {
            if (pair.isEmpty()) continue;
            int i = pair.indexOf('=');
            String k = URLDecoder.decode(i < 0 ? pair : pair.substring(0, i), StandardCharsets.UTF_8);
            String v = i < 0 ? "" : URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
            out.put(k, v);
        }
        return out;
    }

    private static int intParam(Map<String, String> q, String name, int def) {
        String v = q.get(name);
        return v == null || v.isBlank() ? def : Integer.parseInt(v);
    }
}
