package io.chronodim.core.engine;

import io.chronodim.api.Column;
import io.chronodim.api.Op;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.api.VersionCursor;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.KvSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Read-path implementation (R-READ-1..5). All operations run against one
 * {@link KvSnapshot}, so no reader ever observes a torn transaction (R-READ-6).
 *
 * <p>Entity chains stream in key order: validFrom DESC, then txTime DESC within a
 * validFrom group. "Latest knowledge" therefore means: take the first record of
 * each validFrom group.
 */
final class ReadOps {
    private ReadOps() {}

    // ---- single-entity reads ----------------------------------------------------

    static Optional<Version> getCurrent(KvSnapshot snap, TableRuntime rt, Map<String, Object> businessKey) {
        byte[] prefix = entityPrefix(snap, rt, businessKey);
        if (prefix == null) return Optional.empty();
        try (CloseableKvIterator it = snap.prefixScan(prefix)) {
            if (!it.hasNext()) return Optional.empty();
            KV kv = it.next(); // newest validFrom, newest belief
            Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
            Codecs.DecodedValue v = Codecs.decodeValue(kv.value(), sv -> rt.config().schemaAt(sv));
            if (v.op() == Op.DELETE.code || v.validTo() != Version.OPEN) return Optional.empty();
            return Optional.of(toVersion(rt, k, v, true));
        }
    }

    static Optional<Version> getAsOf(KvSnapshot snap, TableRuntime rt, Map<String, Object> businessKey, long validTime) {
        return getAsOf(snap, rt, businessKey, validTime, Long.MAX_VALUE);
    }

    /**
     * Bitemporal read (R-READ-5). Records stream (validFrom DESC, txTime DESC), so
     * the first record with {@code validFrom <= validTime && txTime <= asOfTx} is
     * exactly the floor validity group's newest belief visible at that tx time:
     * everything skipped before it is either a later validity group or a newer
     * belief that did not exist yet.
     */
    static Optional<Version> getAsOf(KvSnapshot snap, TableRuntime rt, Map<String, Object> businessKey,
                                     long validTime, long asOfTx) {
        byte[] prefix = entityPrefix(snap, rt, businessKey);
        if (prefix == null) return Optional.empty();
        try (CloseableKvIterator it = snap.prefixScan(prefix)) {
            while (it.hasNext()) {
                KV kv = it.next();
                Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                if (k.validFrom() > validTime) continue;
                if (k.txTime() > asOfTx) continue;
                Codecs.DecodedValue v = Codecs.decodeValue(kv.value(), sv -> rt.config().schemaAt(sv));
                if (v.op() == Op.DELETE.code) return Optional.empty();
                if (validTime >= v.validTo()) return Optional.empty();
                boolean current = asOfTx == Long.MAX_VALUE && v.validTo() == Version.OPEN;
                return Optional.of(toVersion(rt, k, v, current));
            }
            return Optional.empty();
        }
    }

    static List<Version> getHistory(KvSnapshot snap, TableRuntime rt, Map<String, Object> businessKey) {
        byte[] prefix = entityPrefix(snap, rt, businessKey);
        if (prefix == null) return List.of();
        List<Version> out = new ArrayList<>();
        try (CloseableKvIterator it = snap.prefixScan(prefix)) {
            boolean first = true;
            while (it.hasNext()) {
                KV kv = it.next();
                Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                Codecs.DecodedValue v = Codecs.decodeValue(kv.value(), sv -> rt.config().schemaAt(sv));
                boolean current = first && v.op() != Op.DELETE.code && v.validTo() == Version.OPEN;
                out.add(toVersion(rt, k, v, current));
                first = false;
            }
        }
        return out;
    }

    // ---- table scans -----------------------------------------------------------------

    /** Streaming scan of current versions: first record per entity, if open and non-deleted. */
    static VersionCursor scanCurrent(KvSnapshot snap, TableRuntime rt) {
        return new ScanCursor(snap, rt, Long.MAX_VALUE, true);
    }

    static VersionCursor scanAsOf(KvSnapshot snap, TableRuntime rt, long validTime) {
        return new ScanCursor(snap, rt, validTime, false);
    }

    private static final class ScanCursor implements VersionCursor {
        private final KvSnapshot snap;
        private final TableRuntime rt;
        private final CloseableKvIterator it;
        private final long validTime;
        private final boolean currentOnly;
        private Version next;
        private long curHash;
        private int curDisambig;
        private boolean entityActive;   // still looking for this entity's answer
        private boolean anyEntity;
        private boolean closed;

        ScanCursor(KvSnapshot snap, TableRuntime rt, long validTime, boolean currentOnly) {
            this.snap = snap;
            this.rt = rt;
            this.validTime = validTime;
            this.currentOnly = currentOnly;
            this.it = snap.prefixScan(Codecs.tableDataPrefix(rt.tableId()));
            advance();
        }

        private void advance() {
            next = null;
            while (it.hasNext()) {
                KV kv = it.next();
                Codecs.DataKey k = Codecs.decodeDataKey(kv.key());
                boolean newEntity = !anyEntity || k.keyHash() != curHash || k.disambig() != curDisambig;
                if (newEntity) {
                    anyEntity = true;
                    curHash = k.keyHash();
                    curDisambig = k.disambig();
                    entityActive = true;
                }
                if (!entityActive) continue;

                if (currentOnly) {
                    // Only the very first record of the entity can be current.
                    entityActive = false;
                    if (!newEntity) continue;
                    Codecs.DecodedValue v = Codecs.decodeValue(kv.value(), sv -> rt.config().schemaAt(sv));
                    if (v.op() == Op.DELETE.code || v.validTo() != Version.OPEN) continue;
                    next = toVersion(rt, k, v, true);
                    return;
                }

                // as-of scan: skip groups newer than validTime; the first record with
                // validFrom <= validTime is the floor group's latest belief.
                if (k.validFrom() > validTime) continue;
                entityActive = false;
                Codecs.DecodedValue v = Codecs.decodeValue(kv.value(), sv -> rt.config().schemaAt(sv));
                if (v.op() == Op.DELETE.code || validTime >= v.validTo()) continue;
                next = toVersion(rt, k, v, v.validTo() == Version.OPEN);
                return;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Version next() {
            if (next == null) throw new NoSuchElementException();
            Version v = next;
            advance();
            return v;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                it.close();
                snap.close();
            }
        }
    }

    // ---- helpers -----------------------------------------------------------------------

    /** Resolves the entity's data prefix via the keymap, or null when unknown. */
    private static byte[] entityPrefix(KvSnapshot snap, TableRuntime rt, Map<String, Object> businessKey) {
        List<Column> bkCols = rt.businessKeyColumns();
        Object[] vals = new Object[bkCols.size()];
        for (int i = 0; i < bkCols.size(); i++) {
            Column c = bkCols.get(i);
            Object raw = businessKey.get(c.name());
            if (raw == null) throw new ValidationException("business key column '" + c.name() + "' missing");
            vals[i] = c.type().coerce(raw);
        }
        byte[] bkBytes = Codecs.encodeBusinessKey(bkCols, vals);
        byte[] mapped = snap.get(Codecs.keymapKey(rt.tableId(), bkBytes));
        if (mapped == null) return null;
        Codecs.KeyRef ref = Codecs.decodeKeymapValue(mapped);
        return Codecs.entityPrefix(rt.tableId(), ref.keyHash(), ref.disambig());
    }

    static Version toVersion(TableRuntime rt, Codecs.DataKey k, Codecs.DecodedValue v, boolean current) {
        TableSchema written = rt.config().schemaAt(v.schemaVersion());
        TableSchema currentSchema = rt.config().currentSchema();
        Map<String, Object> row = new LinkedHashMap<>();
        for (Column col : currentSchema.columns()) {
            int idx = written.indexOf(col.name());
            row.put(col.name(), idx >= 0 ? v.payload()[idx] : null);
        }
        return new Version(rt.config().table(), k.validFrom(), v.validTo(), v.txTime(), v.txnId(),
                Op.fromCode(v.op()), v.attrHash(), v.schemaVersion(), current, row);
    }
}
