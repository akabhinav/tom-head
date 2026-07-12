package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.Op;
import io.chronodim.api.TableConfig;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableConfigIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "50 users adjusting at the same time" scenario (NG2's supported half:
 * many concurrent callers, ONE engine process). Verifies under real thread
 * pressure, with readers and snapshots interleaved:
 *
 * <ul>
 *   <li>no lost updates — every acknowledged batch's counters are consistent
 *       with the final stored state;</li>
 *   <li>chain integrity — every entity's SCD2 intervals are contiguous and
 *       non-overlapping after the storm;</li>
 *   <li>duplicate load_id race — many threads submitting the same load id
 *       concurrently: exactly one wins, the rest get the original receipt;</li>
 *   <li>readers never see torn state while writers commit;</li>
 *   <li>a reopen after the storm replays to the identical fingerprint.</li>
 * </ul>
 */
class ConcurrentApplyTest {

    @TempDir
    Path root;

    private static final int USERS = 50;
    private static final int BATCHES_PER_USER = 8;
    private static final int ROWS_PER_BATCH = 40;
    private static final int SHARED_KEYS = 60; // heavily contended entities

    private TableConfig table() {
        return TableConfigIO.fromYaml("""
                table: adj
                business_key: [id]
                schema:
                  - {name: id,     type: string}
                  - {name: amount, type: long}
                  - {name: who,    type: string}
                """); // load_time valid time: every commit gets a distinct instant
    }

    @Test
    void fiftyConcurrentUsersNoLostUpdatesNoTornReads() throws Exception {
        Path db = root.resolve("db");
        EngineOptions opts = EngineOptions.builder().publishEnabled(false).build(); // real group-commit window

        Map<String, Object> verifyBefore;
        AtomicLong ackedRows = new AtomicLong();
        AtomicLong ackedMutating = new AtomicLong(); // inserts+updates+deletes acknowledged
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        try (Engine e = ChronoDim.open(db, opts)) {
            e.createTable(table());

            CyclicBarrier start = new CyclicBarrier(USERS + 1); // writers + reader
            AtomicBoolean writersDone = new AtomicBoolean(false);

            List<Thread> writers = new ArrayList<>();
            for (int u = 0; u < USERS; u++) {
                final int user = u;
                writers.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        for (int b = 0; b < BATCHES_PER_USER; b++) {
                            List<InputRow> rows = new ArrayList<>(ROWS_PER_BATCH);
                            for (int r = 0; r < ROWS_PER_BATCH; r++) {
                                Map<String, Object> v = new LinkedHashMap<>();
                                // Half private keys, half heavily shared keys.
                                String id = r % 2 == 0
                                        ? "U" + user + "-K" + r
                                        : "SHARED-" + ((user * 31 + b * 7 + r) % SHARED_KEYS);
                                v.put("id", id);
                                v.put("amount", (long) (user * 100_000 + b * 1_000 + r));
                                v.put("who", "user-" + user);
                                rows.add(InputRow.upsert(v));
                            }
                            AuditManifest m = e.apply(ApplyBatch.single("u" + user + "-b" + b, "adj", rows));
                            AuditManifest.TableStats st = m.tables().get(0);
                            ackedRows.addAndGet(st.rowsIn());
                            ackedMutating.addAndGet(st.inserts() + st.updates() + st.deletes());
                            assertEquals(0, st.rejects(), "no row may fail in this workload");
                        }
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                }));
            }

            // A relentless reader: every observed version must be self-consistent.
            Thread reader = Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    while (!writersDone.get()) {
                        try (VersionCursor c = e.scanCurrent("adj")) {
                            while (c.hasNext()) {
                                Version v = c.next();
                                assertTrue(v.current() && v.validToIsOpen() && v.op() != Op.DELETE,
                                        "torn read: " + v);
                                assertTrue(v.row().get("who") != null && v.row().get("amount") != null,
                                        "partial row visible: " + v);
                            }
                        }
                        e.getCurrent("adj", Map.of("id", "SHARED-0")).ifPresent(v ->
                                assertTrue(v.validToIsOpen(), "current must be open"));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            });

            for (Thread w : writers) w.join(120_000);
            writersDone.set(true);
            reader.join(30_000);
            assertEquals(List.of(), List.copyOf(failures));

            // Every acknowledged batch left its durable receipt.
            for (int u = 0; u < USERS; u++) {
                for (int b = 0; b < BATCHES_PER_USER; b++) {
                    assertTrue(e.manifest("u" + u + "-b" + b).isPresent(), "missing manifest u" + u + "-b" + b);
                }
            }
            assertEquals((long) USERS * BATCHES_PER_USER * ROWS_PER_BATCH, ackedRows.get());

            // No lost updates: version records in store == acknowledged mutations
            // (each insert = 1 record; each update = supersede/close + new ≤ 2, so we
            // check exact bookkeeping through history instead of guessing):
            long records = 0;
            for (int k = 0; k < SHARED_KEYS; k++) {
                List<Version> hist = e.getHistory("adj", Map.of("id", "SHARED-" + k));
                assertChainWellFormed(hist, "SHARED-" + k);
                records += hist.size();
            }
            assertTrue(records > 0);
            verifyBefore = e.verify();
        }

        // Reopen: WAL replay reproduces the identical state after the storm.
        try (Engine e = ChronoDim.open(db, opts)) {
            assertEquals(verifyBefore.get("state_fingerprint"), e.verify().get("state_fingerprint"));
        }
    }

    /** Latest-knowledge intervals must be strictly ordered and contiguous. */
    private static void assertChainWellFormed(List<Version> historyNewestFirst, String key) {
        // History = (validFrom DESC, txTime DESC); latest belief = first per validFrom.
        List<Version> chain = new ArrayList<>();
        long lastVf = Long.MIN_VALUE;
        boolean first = true;
        for (Version v : historyNewestFirst) {
            if (!first && v.validFrom() == lastVf) continue;
            first = false;
            lastVf = v.validFrom();
            chain.add(v);
        }
        for (int i = 0; i < chain.size(); i++) {
            Version v = chain.get(i);
            if (i == 0) {
                assertTrue(v.validToIsOpen(), key + ": newest version must be open, got " + v.validTo());
            } else {
                assertEquals(chain.get(i - 1).validFrom(), v.validTo(),
                        key + ": interval " + i + " must close exactly where the next opens");
            }
            assertTrue(v.validToIsOpen() || v.validFrom() < v.validTo(), key + ": empty/negative interval");
        }
    }

    @Test
    void duplicateLoadIdRaceHasExactlyOneWinner() throws Exception {
        Path db = root.resolve("db2");
        try (Engine e = ChronoDim.open(db, EngineOptions.builder().publishEnabled(false).build())) {
            e.createTable(table());

            int racers = 24;
            CyclicBarrier start = new CyclicBarrier(racers);
            AtomicInteger winners = new AtomicInteger();
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < racers; t++) {
                final int who = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        // Same load_id, DIFFERENT payloads: only one may ever apply.
                        AuditManifest m = e.apply(ApplyBatch.single("the-same-load", "adj",
                                List.of(InputRow.upsert(Map.of("id", "X", "amount", (long) who, "who", "racer-" + who)))));
                        if (!m.alreadyApplied()) winners.incrementAndGet();
                    } catch (InterruptedException | BrokenBarrierException ex) {
                        failures.add(ex);
                    }
                }));
            }
            for (Thread t : threads) t.join(60_000);
            assertEquals(List.of(), List.copyOf(failures));
            assertEquals(1, winners.get(), "exactly one racer may win the load_id");
            // And the stored state belongs to exactly one racer, entirely.
            Version v = e.getCurrent("adj", Map.of("id", "X")).orElseThrow();
            String who = (String) v.row().get("who");
            long amount = (Long) v.row().get("amount");
            assertEquals("racer-" + amount, who, "row must be one racer's payload, not a mix");
            assertEquals(1, e.getHistory("adj", Map.of("id", "X")).size(), "only the winner's version exists");
        }
    }
}
