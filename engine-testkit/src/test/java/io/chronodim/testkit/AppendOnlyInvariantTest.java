package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import io.chronodim.api.Version;
import io.chronodim.core.codec.Codecs;
import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.wal.TxnPayload;
import io.chronodim.core.wal.Wal;
import io.chronodim.core.wal.WalReader;
import io.chronodim.storage.AtomicBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The append-only guarantee (G6, §5.3) — the anti-"delete-and-rewrite" test.
 *
 * <p>Unlike a lakehouse MERGE (copy-on-write file rewrites, physical row
 * deletes, VACUUM erasing history), ChronoDim must never destroy a committed
 * version record: row deletes become DELETE tombstone *versions*, corrections
 * append superseding records. This test drives updates, soft deletes,
 * re-inserts and late corrections, then audits the physical WAL: every mutation
 * against versioned keyspaces must be a PUT, and no data record written by one
 * transaction may ever be overwritten by a later one.
 */
class AppendOnlyInvariantTest {

    @TempDir
    Path dir;

    @Test
    void versionedKeyspacesAreAppendOnlyAtThePhysicalLayer() {
        TableConfig cfg = DataGen.testTable("t");
        DataGen gen = new DataGen(17, new DataGen.StreamOptions(12, 0.2, 0.15, 0.05, 0.2));

        Map<String, Object> bkOfDeleted = null;
        try (Engine e = ChronoDim.open(dir, EngineOptions.builder().groupCommitWindowMicros(0).publishEnabled(false).build())) {
            e.createTable(cfg);
            for (int b = 0; b < 12; b++) {
                e.apply(ApplyBatch.single("b" + b, "t", gen.nextBatch(30)));
            }
            // Find an entity that is currently soft-deleted and prove its history survives.
            for (int k = 1; k <= 12 && bkOfDeleted == null; k++) {
                Map<String, Object> bk = Map.of("customer_id", "C" + k);
                List<Version> hist = e.getHistory("t", bk);
                if (!hist.isEmpty() && e.getCurrent("t", bk).isEmpty()) {
                    bkOfDeleted = bk;
                    assertTrue(hist.size() >= 2, "deleted entity must keep its pre-delete versions");
                    assertTrue(hist.stream().anyMatch(v -> v.op() == io.chronodim.api.Op.DELETE),
                            "soft delete must be a tombstone VERSION, not a physical removal");
                    assertTrue(hist.stream().anyMatch(v -> v.op() != io.chronodim.api.Op.DELETE),
                            "the deleted data itself must still be present in history");
                }
            }
        }

        // Physical audit: replay the WAL and inspect every mutation ever committed.
        Map<Byte, Long> putsByKeyspace = new HashMap<>();
        List<String> violations = new ArrayList<>();
        Map<String, Long> dataKeyWriter = new LinkedHashMap<>(); // data key hex -> txn that wrote it
        WalReader.scan(dir.resolve(EngineImpl.WAL_DIR), false, rec -> {
            if (rec.type() != Wal.TXN_COMMIT && rec.type() != Wal.CONFIG_CHANGE) return;
            TxnPayload.Decoded d = TxnPayload.decode(rec.payload());
            for (AtomicBatch.Mutation m : d.batch().mutations()) {
                byte ks = m.key()[0];
                if (m instanceof AtomicBatch.Delete) {
                    if (ks != Codecs.KS_QUAR) {
                        violations.add("txn " + rec.txnId() + ": DELETE against keyspace 0x"
                                + Integer.toHexString(ks) + " — versioned keyspaces must be append-only");
                    }
                    continue;
                }
                putsByKeyspace.merge(ks, 1L, Long::sum);
                if (ks == Codecs.KS_DATA) {
                    String keyHex = io.chronodim.storage.util.Bytes.hex(m.key());
                    Long prior = dataKeyWriter.putIfAbsent(keyHex, rec.txnId());
                    if (prior != null && prior != rec.txnId()) {
                        violations.add("data key " + keyHex + " written by txn " + prior
                                + " overwritten by txn " + rec.txnId() + " — history destroyed");
                    }
                }
            }
        });

        assertEquals(List.of(), violations);
        assertTrue(putsByKeyspace.getOrDefault(Codecs.KS_DATA, 0L) > 100,
                "expected a substantial number of version records, got " + putsByKeyspace);
    }
}
