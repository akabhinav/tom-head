package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.durability.ObjectStore;
import io.chronodim.durability.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Disaster drill (R-DUR-6): write load → snapshot to object store → keep writing →
 * destroy the data directory entirely → restore from the store → state matches.
 */
class DisasterDrillTest {

    @TempDir
    Path root;

    @Test
    void fullDrillWithPointInTimeRestore() throws IOException {
        Path db = root.resolve("db");
        Path store = root.resolve("bucket");
        Path restored = root.resolve("restored");
        Path restoredAsOf = root.resolve("restored-asof");

        EngineOptions opts = EngineOptions.builder()
                .groupCommitWindowMicros(0)
                .objectStoreUri(store.toString())
                .walShipIntervalMillis(50)
                .publishEnabled(false)
                .build();

        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 refAll = new ReferenceScd2(cfg);
        ReferenceScd2 refAtSnapshot = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(31, DataGen.StreamOptions.defaults());

        long snapshotTxn;
        Map<String, Object> verifyBefore;
        try (Engine e = ChronoDim.open(db, opts)) {
            e.createTable(cfg);
            for (int b = 0; b < 6; b++) {
                List<InputRow> rows = gen.nextBatch(25);
                AuditManifest m = e.apply(ApplyBatch.single("pre-" + b, "t", rows));
                refAll.applyBatch(rows, m.startedAtMicros());
                refAtSnapshot.applyBatch(rows, m.startedAtMicros());
            }
            // Snapshot + upload (R-DUR-2)
            Map<String, Object> snap = e.snapshot();
            snapshotTxn = ((Number) snap.get("txn_id")).longValue();
            SnapshotManager.upload(ObjectStore.open(store.toString()), Path.of(String.valueOf(snap.get("path"))), snapshotTxn);

            // More writes after the snapshot — must come back via shipped WAL.
            for (int b = 0; b < 5; b++) {
                List<InputRow> rows = gen.nextBatch(25);
                AuditManifest m = e.apply(ApplyBatch.single("post-" + b, "t", rows));
                refAll.applyBatch(rows, m.startedAtMicros());
            }
            verifyBefore = e.verify();
        } // close ships the final WAL bytes (clean-shutdown RPO = 0)

        // 💥 the machine is gone
        deleteRecursive(db);

        // Full restore: snapshot + complete WAL replay
        Map<String, Object> report = SnapshotManager.restore(ObjectStore.open(store.toString()), restored, null);
        assertEquals(snapshotTxn, ((Number) report.get("snapshot_txn")).longValue());
        try (Engine e = ChronoDim.open(restored, EngineOptions.builder().groupCommitWindowMicros(0).publishEnabled(false).build())) {
            DifferentialTest.compareFullState(e, refAll, cfg);
            assertEquals(verifyBefore.get("state_fingerprint"), e.verify().get("state_fingerprint"),
                    "restored fingerprint must match pre-disaster fingerprint (R-DUR-5)");
        }

        // Point-in-time restore to the snapshot txn: exactly the pre-snapshot state.
        SnapshotManager.restore(ObjectStore.open(store.toString()), restoredAsOf, snapshotTxn);
        try (Engine e = ChronoDim.open(restoredAsOf, EngineOptions.builder().groupCommitWindowMicros(0).publishEnabled(false).build())) {
            assertTrue(e.lastCommittedTxn() <= snapshotTxn);
            DifferentialTest.compareFullState(e, refAtSnapshot, cfg);
        }
    }

    private static void deleteRecursive(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> walk = Files.walk(p)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.deleteIfExists(f);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
    }
}
