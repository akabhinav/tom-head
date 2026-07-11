package io.chronodim.core.scd2;

import io.chronodim.api.InputRow;
import io.chronodim.api.Op;
import io.chronodim.api.TableConfig;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;
import io.chronodim.core.util.ExternalSorter;
import io.chronodim.storage.KV;
import io.chronodim.storage.StorageEngine;
import io.chronodim.storage.util.XxHash64;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Bulk backfill (R-APPLY-8): initial table load through the sorted-ingest path.
 *
 * <p>Two-pass: pass 1 coerces and spills rows into an external sort keyed by
 * (keyHash, businessKey, validFrom); pass 2 streams sorted rows, builds each
 * entity's SCD2 chain, and feeds two sorted-ingest streams (data records, then
 * keymap/hashreg records) directly into the storage engine — the memtable and
 * per-row commit pipeline are bypassed entirely. The caller fences the load with
 * BACKFILL_BEGIN/END WAL records and commits the manifest afterwards.
 */
public final class BackfillLoader {

    public static Scd2Applier.Counters load(TableRuntime rt, Iterable<InputRow> rows, long txTime, long txnId,
                                            StorageEngine storage, Path tmpDir, long spillBytes,
                                            Scd2Applier.ErrorSink errors) {
        TableConfig cfg = rt.config();
        TableSchema schema = cfg.currentSchema();
        Scd2Applier.Counters c = new Scd2Applier.Counters();

        try (ExternalSorter sorter = new ExternalSorter(tmpDir, spillBytes);
             ExternalSorter secondary = new ExternalSorter(tmpDir.resolve("secondary"), spillBytes)) {

            long rowIndex = -1;
            for (InputRow in : rows) {
                rowIndex++;
                c.rowsIn++;
                try {
                    Scd2Applier.Coerced cr = Scd2Applier.coerceRow(rt, in, txTime);
                    String gate = Scd2Applier.gateFailure(cfg, cr.values());
                    if (gate != null) throw new ValidationException(gate);
                    if (cr.delete() && cfg.deleteMode() == TableConfig.DeleteMode.IGNORE) {
                        c.noOps++;
                        continue;
                    }
                    long hash = XxHash64.hash(cr.bkBytes());
                    byte[] sortKey = sortKey(hash, cr.bkBytes(), cr.validFrom());
                    // Temp record: [delete flag][full value blob with validFrom parked in the
                    // validTo header slot]; pass 2 patches the real header in.
                    byte[] blob = Codecs.encodeValue(Op.INSERT, cr.validFrom(), 0, 0,
                            schema.schemaVersion(), cr.attrHash(), cr.bkBytes(), schema, cr.values());
                    byte[] tmp = new byte[blob.length + 1];
                    tmp[0] = (byte) (cr.delete() ? 1 : 0);
                    System.arraycopy(blob, 0, tmp, 1, blob.length);
                    sorter.add(sortKey, tmp);
                } catch (ValidationException e) {
                    if (cfg.batchFailurePolicy() == TableConfig.BatchFailurePolicy.FAIL_BATCH) {
                        throw new ValidationException("backfill row " + rowIndex + ": " + e.getMessage() + " (policy=fail_batch)");
                    }
                    c.rejects++;
                    errors.error(cfg.table(), rowIndex, e.getMessage());
                }
            }

            // Pass 2: entity chains → data ingest, with secondary keys collected en route.
            DataStream data = new DataStream(rt, sorter.sortedIterator(), txTime, txnId, c, secondary);
            storage.ingestSorted(data, txnId, Math.max(1024, sorter.count()));
            storage.ingestSorted(secondary.sortedIterator(), txnId, Math.max(1024, data.entities * 2L));
        }
        return c;
    }

    private static byte[] sortKey(long hash, byte[] bk, long validFrom) {
        ByteBuffer b = ByteBuffer.allocate(8 + 2 + bk.length + 8);
        b.putLong(hash).putShort((short) bk.length).put(bk).putLong(validFrom ^ Long.MIN_VALUE);
        return b.array();
    }

    /** Streams data KVs entity-by-entity in exact key order (hash asc, disambig asc, validFrom desc). */
    private static final class DataStream implements Iterator<KV> {
        private final TableRuntime rt;
        private final Iterator<KV> sorted;
        private final long txTime;
        private final long txnId;
        private final Scd2Applier.Counters c;
        private final ExternalSorter secondary;

        private long currentHash;
        private boolean anyHash;
        private int nextDisambig;
        private final List<KV> pendingEntity = new ArrayList<>();
        private int emitIdx;
        private KV lookahead;
        long entities;

        DataStream(TableRuntime rt, Iterator<KV> sorted, long txTime, long txnId,
                   Scd2Applier.Counters c, ExternalSorter secondary) {
            this.rt = rt;
            this.sorted = sorted;
            this.txTime = txTime;
            this.txnId = txnId;
            this.c = c;
            this.secondary = secondary;
        }

        @Override
        public boolean hasNext() {
            while (emitIdx >= pendingEntity.size()) {
                pendingEntity.clear();
                emitIdx = 0;
                if (!buildNextEntity()) return false;
            }
            return true;
        }

        @Override
        public KV next() {
            if (!hasNext()) throw new NoSuchElementException();
            return pendingEntity.get(emitIdx++);
        }

        /** Pulls all sorted rows of one entity and materializes its chain records. */
        private boolean buildNextEntity() {
            KV first = lookahead != null ? lookahead : (sorted.hasNext() ? sorted.next() : null);
            lookahead = null;
            if (first == null) return false;

            long hash = ByteBuffer.wrap(first.key()).getLong();
            byte[] bk = bkOf(first.key());
            List<KV> rows = new ArrayList<>();
            rows.add(first);
            while (sorted.hasNext()) {
                KV kv = sorted.next();
                if (ByteBuffer.wrap(kv.key()).getLong() != hash || !Arrays.equals(bkOf(kv.key()), bk)) {
                    lookahead = kv;
                    break;
                }
                rows.add(kv);
            }

            if (!anyHash || hash != currentHash) {
                currentHash = hash;
                anyHash = true;
                nextDisambig = 0;
            }
            int disambig = nextDisambig++;
            entities++;

            // Last-wins for identical validFrom within the load.
            List<KV> deduped = new ArrayList<>(rows.size());
            for (KV kv : rows) {
                long vf = validFromOf(kv.key());
                if (!deduped.isEmpty() && validFromOf(deduped.getLast().key()) == vf) {
                    deduped.set(deduped.size() - 1, kv);
                } else {
                    deduped.add(kv);
                }
            }

            // Build the chain ascending, then emit descending (data keys are ~validFrom).
            List<byte[]> values = new ArrayList<>();
            List<Long> vfs = new ArrayList<>();
            long prevHash = 0;
            byte prevOp = 0;
            boolean havePrev = false;
            for (KV kv : deduped) {
                boolean delete = kv.value()[0] == 1;
                byte[] blob = Arrays.copyOfRange(kv.value(), 1, kv.value().length);
                Codecs.DecodedValue header = Codecs.decodeHeader(blob);
                long vf = header.validTo(); // validFrom parked here by pass 1
                if (havePrev && !delete && prevOp != Op.DELETE.code && header.attrHash() == prevHash) {
                    c.noOps++;
                    continue;
                }
                if (delete && (!havePrev || prevOp == Op.DELETE.code)) {
                    c.noOps++;
                    continue;
                }
                Op op = delete ? Op.DELETE : (!havePrev || prevOp == Op.DELETE.code ? Op.INSERT : Op.UPDATE);
                // A tombstone carries the deleted version's payload (same as the apply path).
                byte[] base = delete ? values.get(values.size() - 1) : blob;
                if (havePrev) {
                    int last = values.size() - 1;
                    values.set(last, Codecs.withNewValidTo(values.get(last), vf, txTime, txnId));
                }
                values.add(Codecs.patchHeader(base, op, io.chronodim.api.Version.OPEN, txTime, txnId));
                vfs.add(vf);
                prevHash = header.attrHash();
                prevOp = op.code;
                havePrev = true;
                switch (op) {
                    case INSERT -> c.inserts++;
                    case UPDATE -> c.updates++;
                    case DELETE -> c.deletes++;
                }
            }
            if (values.isEmpty()) {
                // Everything deduped to nothing (e.g. only no-op deletes): entity vanishes.
                nextDisambig--;
                entities--;
                return true; // pendingEntity stays empty; hasNext() loops on
            }

            for (int i = values.size() - 1; i >= 0; i--) {
                byte[] key = Codecs.dataKey(rt.tableId(), hash, disambig, vfs.get(i), txTime);
                pendingEntity.add(new KV(key, values.get(i)));
            }
            secondary.add(Codecs.keymapKey(rt.tableId(), bk), Codecs.keymapValue(hash, disambig));
            secondary.add(Codecs.hashregKey(rt.tableId(), hash, disambig), bk);
            return true;
        }

        private static byte[] bkOf(byte[] sortKey) {
            ByteBuffer b = ByteBuffer.wrap(sortKey);
            b.getLong();
            byte[] bk = new byte[b.getShort() & 0xFFFF];
            b.get(bk);
            return bk;
        }

        private static long validFromOf(byte[] sortKey) {
            ByteBuffer b = ByteBuffer.wrap(sortKey);
            b.getLong();
            int len = b.getShort() & 0xFFFF;
            b.position(b.position() + len);
            return b.getLong() ^ Long.MIN_VALUE;
        }
    }

    private BackfillLoader() {}
}
