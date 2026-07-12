package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.core.wal.WalReader;
import io.chronodim.durability.ObjectStore;
import io.chronodim.durability.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WAL pruning: bounded local disk over long deployments without ever weakening
 * recovery — only segments that are durable in storage, released by the
 * publisher, and fully shipped may be deleted; the active segment never is.
 */
class WalPruneTest {

    @TempDir
    Path root;

    private static EngineOptions.Builder base() {
        return EngineOptions.builder()
                .groupCommitWindowMicros(0)
                .walSegmentBytes(64 << 10); // tiny segments → plenty to prune
    }

    @Test
    void pruneKeepsStateAndRecoveryIntact() {
        Path db = root.resolve("db");
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(23, DataGen.StreamOptions.defaults());

        try (Engine e = ChronoDim.open(db, base().publishEnabled(false).build())) {
            e.createTable(cfg);
            for (int b = 0; b < 20; b++) {
                List<InputRow> rows = gen.nextBatch(40);
                AuditManifest m = e.apply(ApplyBatch.single("b" + b, "t", rows));
                ref.applyBatch(rows, m.startedAtMicros());
            }
            int before = WalReader.segments(db.resolve("wal")).size();
            assertTrue(before > 3, "need several segments, got " + before);

            // Without a checkpoint nothing is durable → nothing prunes.
            Map<String, Object> noop = e.pruneWal();
            assertEquals(List.of(), noop.get("pruned"));

            e.snapshot(); // advances the durable watermark
            Map<String, Object> result = e.pruneWal();
            assertTrue(((List<?>) result.get("pruned")).size() >= before - 2, result.toString());
            assertTrue(WalReader.segments(db.resolve("wal")).size() >= 1, "active segment always kept");
        }

        // Reopen: recovery works from the remaining tail; state identical.
        try (Engine e = ChronoDim.open(db, base().publishEnabled(false).build())) {
            DifferentialTest.compareFullState(e, ref, cfg);

            // And the engine keeps working after pruning.
            List<InputRow> rows = gen.nextBatch(20);
            AuditManifest m = e.apply(ApplyBatch.single("after-prune", "t", rows));
            ref.applyBatch(rows, m.startedAtMicros());
            DifferentialTest.compareFullState(e, ref, cfg);
        }
    }

    @Test
    void shipperVetoUntilUploadedAndRestoreStillWorks() {
        Path db = root.resolve("db2");
        Path store = root.resolve("bucket");
        Path restored = root.resolve("restored");
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(29, DataGen.StreamOptions.defaults());

        EngineOptions opts = base()
                .publishEnabled(false)
                .objectStoreUri(store.toString())
                .walShipIntervalMillis(60_000) // shipping only happens via the prune-time sync attempt
                .build();

        Map<String, Object> verifyBefore;
        try (Engine e = ChronoDim.open(db, opts)) {
            e.createTable(cfg);
            for (int b = 0; b < 12; b++) {
                List<InputRow> rows = gen.nextBatch(40);
                AuditManifest m = e.apply(ApplyBatch.single("b" + b, "t", rows));
                ref.applyBatch(rows, m.startedAtMicros());
            }
            Map<String, Object> snap = e.snapshot();
            SnapshotManager.upload(ObjectStore.open(store.toString()),
                    Path.of(String.valueOf(snap.get("path"))), ((Number) snap.get("txn_id")).longValue());

            // Prune forces a synchronous ship of every segment it wants to drop.
            Map<String, Object> result = e.pruneWal();
            assertTrue(((List<?>) result.get("pruned")).size() >= 1, result.toString());
            for (Object name : (List<?>) result.get("pruned")) {
                assertTrue(ObjectStore.open(store.toString()).exists("wal/" + name),
                        "pruned segment must exist in the object store: " + name);
            }
            verifyBefore = e.verify();
        }

        // Even with local segments pruned, a full disaster restore succeeds
        // because the archive copy is complete.
        Map<String, Object> report = SnapshotManager.restore(ObjectStore.open(store.toString()), restored, null);
        assertTrue(((Number) report.get("snapshot_txn")).longValue() > 0);
        try (Engine e = ChronoDim.open(restored, base().publishEnabled(false).build())) {
            DifferentialTest.compareFullState(e, ref, cfg);
            assertEquals(verifyBefore.get("state_fingerprint"), e.verify().get("state_fingerprint"));
        }
    }
}
