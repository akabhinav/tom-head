package io.chronodim.cli;

import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.core.util.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The built-in console end to end over real HTTP: create a table, adjust,
 * browse, drill into history, audit, health — everything the page calls.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UiServerTest {

    @TempDir
    static Path root;

    private static Engine engine;
    private static UiServer server;
    private static String base;
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void up() throws IOException {
        engine = ChronoDim.open(root.resolve("db"), EngineOptions.builder().publishEnabled(false).build());
        server = UiServer.start(engine, "127.0.0.1", 0);
        base = "http://127.0.0.1:" + server.port();
    }

    @AfterAll
    static void down() {
        server.stop();
        engine.close();
    }

    private record Res(int status, String body) {
        Map<String, Object> json() {
            return Json.parseObject(body);
        }
    }

    private static Res get(String path) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return new Res(r.statusCode(), r.body());
    }

    private static Res post(String path, String body) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        return new Res(r.statusCode(), r.body());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(Object o) {
        return (List<Map<String, Object>>) o;
    }

    @Test
    @Order(1)
    void servesThePageAndEmptyCatalog() throws Exception {
        Res page = get("/");
        assertEquals(200, page.status());
        assertTrue(page.body().contains("ChronoDim Console"));
        assertTrue(page.body().contains("<script>"), "the page must carry its own JS — no CDN");
        assertFalse(page.body().contains("http://") || page.body().replace(base, "").contains("https://"),
                "no external resources");

        assertEquals(200, get("/api/tables").status());
        assertEquals(List.of(), get("/api/tables").json().get("tables"));
        assertEquals(404, get("/api/nope").status());
        assertEquals(404, get("/favicon.ico").status());
    }

    @Test
    @Order(2)
    void createTableValidatesAndDescribes() throws Exception {
        Res bad = post("/api/tables", "table: 1bad name\nbusiness_key: [x]\nschema:\n  - {name: x, type: string}");
        assertEquals(400, bad.status());
        assertNotNull(bad.json().get("error"));

        Res ok = post("/api/tables", """
                table: account
                business_key: [id]
                schema:
                  - {name: id,      type: string}
                  - {name: balance, type: "decimal(18,2)"}
                  - {name: owner,   type: string}
                  - {name: tags,    type: "array<string>"}
                on_row_error: quarantine
                quality_gates:
                  - {column: owner, rule: not_null}
                """);
        assertEquals(201, ok.status(), ok.body());
        assertEquals("account", ok.json().get("table"));
        assertEquals(List.of("id"), ok.json().get("business_key"));
        assertEquals(4, list(ok.json().get("columns")).size());
        assertEquals("array<string>", list(ok.json().get("columns")).get(3).get("type"));

        // Duplicate create is a client error, not a 500.
        assertEquals(400, post("/api/tables", "table: account\nbusiness_key: [id]\nschema:\n  - {name: id, type: string}").status());
    }

    @Test
    @Order(3)
    void applyBrowseHistoryRoundTrip() throws Exception {
        Res m = post("/api/tables/account/apply", """
                {
                  "load_id": "ui-1",
                  "metadata": {"approved_by": "ops"},
                  "records": [
                    {"id": "A1", "balance": "100.00", "owner": "ada", "tags": ["vip"]},
                    {"id": "A2", "balance": "55.50",  "owner": "bob"}
                  ]
                }
                """);
        assertEquals(200, m.status(), m.body());
        assertEquals(Boolean.FALSE, m.json().get("already_applied"));
        assertEquals(2L, list(m.json().get("tables")).get(0).get("inserts"));

        // Same load_id again → the original receipt, nothing re-applied.
        Res dup = post("/api/tables/account/apply",
                "{\"load_id\": \"ui-1\", \"records\": [{\"id\": \"A1\", \"balance\": \"999\", \"owner\": \"eve\"}]}");
        assertEquals(200, dup.status());
        assertEquals(Boolean.TRUE, dup.json().get("already_applied"));

        // An update through the form path (bare array + query-param load id).
        Res upd = post("/api/tables/account/apply?load_id=ui-2",
                "[{\"id\": \"A1\", \"balance\": \"250.00\", \"owner\": \"ada\", \"tags\": [\"vip\",\"gold\"]}]");
        assertEquals(200, upd.status(), upd.body());
        assertEquals(1L, list(upd.json().get("tables")).get(0).get("updates"));

        // Browse: A1 shows the new balance; the dup racer's 999/eve never landed.
        Res rows = get("/api/tables/account/rows?limit=10");
        assertEquals(200, rows.status());
        List<Map<String, Object>> rs = list(rows.json().get("rows"));
        assertEquals(2, rs.size());
        Map<String, Object> a1 = rs.stream()
                .filter(r -> "A1".equals(((Map<?, ?>) r.get("row")).get("id"))).findFirst().orElseThrow();
        assertEquals("250.00", ((Map<?, ?>) a1.get("row")).get("balance"));
        assertNull(a1.get("_valid_to"), "current row must be open");

        // Substring filter + paging flags.
        assertEquals(1, list(get("/api/tables/account/rows?q=bob").json().get("rows")).size());
        assertEquals(Boolean.TRUE, get("/api/tables/account/rows?limit=1").json().get("has_more"));

        // History is the full append-only bitemporal log: the update stored a NEW
        // open version plus a superseding "close" of the old belief — the original
        // open record is still there, never rewritten. 3 records, not 2.
        Res hist = get("/api/tables/account/history?id=A1");
        List<Map<String, Object>> vs = list(hist.json().get("versions"));
        assertEquals(3, vs.size());
        assertNull(vs.get(0).get("_valid_to"));                       // new belief: open at vf2
        assertEquals(Boolean.TRUE, vs.get(0).get("_is_current"));
        assertEquals(vs.get(0).get("_valid_from"), vs.get(1).get("_valid_to"),
                "old version must close exactly where the new one opens");
        assertEquals("100.00", ((Map<?, ?>) vs.get(1).get("row")).get("balance"));
        assertNull(vs.get(2).get("_valid_to"), "the superseded original open record is preserved");
        assertEquals(Boolean.FALSE, vs.get(2).get("_is_current"));
        assertEquals(vs.get(1).get("_valid_from"), vs.get(2).get("_valid_from"),
                "close record supersedes the original at the same valid_from");

        assertEquals(400, get("/api/tables/account/history").status(), "missing key column → 400");
        assertEquals(400, get("/api/tables/account/rows?as_of=not-a-time").status());
    }

    @Test
    @Order(4)
    void csvDeleteQuarantineAuditHealth() throws Exception {
        // CSV upload, exactly like a file drop: header + rows, empty cell = null.
        Res csv = post("/api/tables/account/apply?load_id=ui-csv&format=csv",
                "id,balance,owner,_op\nA2,,ada,delete\nA3,10.00,cyd,\n");
        assertEquals(200, csv.status(), csv.body());
        Map<String, Object> st = list(csv.json().get("tables")).get(0);
        assertEquals(1L, st.get("deletes"));
        assertEquals(1L, st.get("inserts"));

        // A2 gone from current, tombstone in history; quality gate quarantines a NULL owner.
        assertEquals(0, list(get("/api/tables/account/rows?q=A2").json().get("rows")).size());
        Res bad = post("/api/tables/account/apply?load_id=ui-bad",
                "[{\"id\": \"A4\", \"balance\": \"1.00\", \"owner\": null}]");
        assertEquals(200, bad.status());
        assertEquals(1L, list(bad.json().get("tables")).get(0).get("quarantined"));
        List<Map<String, Object>> q = list(get("/api/quarantine?table=account").json().get("quarantined"));
        assertEquals(1, q.size());
        assertTrue(String.valueOf(q.get(0).get("reason")).contains("owner"));

        // Audit trail has every load with its metadata.
        List<Map<String, Object>> mans = list(get("/api/manifests?table=account&limit=50").json().get("manifests"));
        assertTrue(mans.size() >= 4);
        assertTrue(mans.stream().anyMatch(mm -> "ui-1".equals(mm.get("load_id"))
                && "ops".equals(((Map<?, ?>) mm.get("metadata")).get("approved_by"))));

        // Health endpoints.
        assertEquals(200, get("/api/stats").status());
        assertEquals(List.of("account"), get("/api/stats").json().get("tables"));
        assertNotNull(get("/api/verify").json().get("state_fingerprint"));
    }

    @Test
    @Order(5)
    void manyBrowserTabsAtOnce() throws Exception {
        // The 50-users story through the HTTP layer: concurrent applies + reads.
        int users = 20;
        CountDownLatch done = new CountDownLatch(users);
        java.util.concurrent.ConcurrentLinkedQueue<Throwable> fails = new java.util.concurrent.ConcurrentLinkedQueue<>();
        for (int u = 0; u < users; u++) {
            final int who = u;
            Thread.ofPlatform().start(() -> {
                try {
                    Res r = post("/api/tables/account/apply?load_id=tab-" + who,
                            "[{\"id\": \"T" + who + "\", \"balance\": \"1.00\", \"owner\": \"tab" + who + "\"}]");
                    assertEquals(200, r.status(), r.body());
                    assertEquals(200, get("/api/tables/account/rows?limit=5").status());
                } catch (Throwable t) {
                    fails.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(List.of(), List.copyOf(fails));
        for (int u = 0; u < users; u++) {
            assertEquals(1, list(get("/api/tables/account/history?id=T" + u).json().get("versions")).size(),
                    "tab " + u + "'s insert must have landed exactly once");
        }
    }
}
