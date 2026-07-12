package io.chronodim.core.codec;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.CorruptionException;
import io.chronodim.api.Op;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;
import io.chronodim.storage.util.XxHash64;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Frozen on-disk encodings (§11, docs/formats.md). Big-endian throughout so byte
 * order equals sort order.
 *
 * <p><b>Keyspaces</b> (first key byte):
 * <pre>
 * 0x01 DATA      [ks][u32 tableId][u64 keyHash][u16 disambig][u64 desc(validFrom)][u64 desc(txTime)]
 * 0x02 KEYMAP    [ks][u32 tableId][bkBytes]                  → [u64 keyHash][u16 disambig]
 * 0x03 META      [ks][u8 sub][...]                            → catalog JSON / counters
 * 0x04 MANIFEST  [ks][u64 txnId]                              → audit manifest JSON
 * 0x05 LOADID    [ks][utf8 loadId]                            → [u64 txnId]
 * 0x06 QUAR      [ks][u32 tableId][u64 seq]                   → quarantine JSON
 * 0x07 HASHREG   [ks][u32 tableId][u64 keyHash][u16 disambig] → bkBytes
 * </pre>
 * {@code desc(x) = ~(x ^ Long.MIN_VALUE)}: order-preserving for signed longs,
 * inverted so newest sorts first.
 *
 * <p><b>DATA value layout</b> (fixed header at fixed offsets, R-STORE-3):
 * <pre>
 * [u8 valueVersion=1][u8 op][i64 validTo][i64 txTime][u64 txnId][u32 schemaVersion]
 * [u64 attrHash][u16 bkLen][bkBytes][payload columns]
 * </pre>
 */
public final class Codecs {
    private Codecs() {}

    public static final byte KS_DATA = 0x01;
    public static final byte KS_KEYMAP = 0x02;
    public static final byte KS_META = 0x03;
    public static final byte KS_MANIFEST = 0x04;
    public static final byte KS_LOADID = 0x05;
    public static final byte KS_QUAR = 0x06;
    public static final byte KS_HASHREG = 0x07;

    public static final byte VALUE_VERSION = 1;
    public static final int DATA_KEY_LEN = 1 + 4 + 8 + 2 + 8 + 8;
    public static final int VALUE_HEADER_LEN = 1 + 1 + 8 + 8 + 8 + 4 + 8 + 2;

    // ---- order-preserving descending encoding ---------------------------------

    public static long desc(long x) {
        return ~(x ^ Long.MIN_VALUE);
    }

    public static long undesc(long e) {
        return (~e) ^ Long.MIN_VALUE;
    }

    // ---- data keys -------------------------------------------------------------

    public static byte[] dataKey(int tableId, long keyHash, int disambig, long validFrom, long txTime) {
        ByteBuffer b = ByteBuffer.allocate(DATA_KEY_LEN);
        b.put(KS_DATA).putInt(tableId).putLong(keyHash).putShort((short) disambig)
                .putLong(desc(validFrom)).putLong(desc(txTime));
        return b.array();
    }

    /** Prefix covering every version of one entity. */
    public static byte[] entityPrefix(int tableId, long keyHash, int disambig) {
        ByteBuffer b = ByteBuffer.allocate(1 + 4 + 8 + 2);
        b.put(KS_DATA).putInt(tableId).putLong(keyHash).putShort((short) disambig);
        return b.array();
    }

    /** Prefix covering every version of every entity of one table. */
    public static byte[] tableDataPrefix(int tableId) {
        ByteBuffer b = ByteBuffer.allocate(1 + 4);
        b.put(KS_DATA).putInt(tableId);
        return b.array();
    }

    public record DataKey(int tableId, long keyHash, int disambig, long validFrom, long txTime) {}

    public static DataKey decodeDataKey(byte[] key) {
        if (key.length != DATA_KEY_LEN || key[0] != KS_DATA) {
            throw new CorruptionException("not a data key (len=" + key.length + ")");
        }
        ByteBuffer b = ByteBuffer.wrap(key, 1, key.length - 1);
        return new DataKey(b.getInt(), b.getLong(), b.getShort() & 0xFFFF, undesc(b.getLong()), undesc(b.getLong()));
    }

    // ---- secondary keys -----------------------------------------------------------

    public static byte[] keymapKey(int tableId, byte[] bkBytes) {
        ByteBuffer b = ByteBuffer.allocate(5 + bkBytes.length);
        b.put(KS_KEYMAP).putInt(tableId).put(bkBytes);
        return b.array();
    }

    public static byte[] keymapValue(long keyHash, int disambig) {
        return ByteBuffer.allocate(10).putLong(keyHash).putShort((short) disambig).array();
    }

    public record KeyRef(long keyHash, int disambig) {}

    public static KeyRef decodeKeymapValue(byte[] v) {
        ByteBuffer b = ByteBuffer.wrap(v);
        return new KeyRef(b.getLong(), b.getShort() & 0xFFFF);
    }

    public static byte[] hashregKey(int tableId, long keyHash, int disambig) {
        return ByteBuffer.allocate(15).put(KS_HASHREG).putInt(tableId).putLong(keyHash).putShort((short) disambig).array();
    }

    public static byte[] metaKey(byte sub, String name) {
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + n.length).put(KS_META).put(sub).put(n).array();
    }

    public static byte[] manifestKey(long txnId) {
        return ByteBuffer.allocate(9).put(KS_MANIFEST).putLong(txnId).array();
    }

    public static byte[] loadIdKey(String loadId) {
        byte[] n = loadId.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + n.length).put(KS_LOADID).put(n).array();
    }

    public static byte[] quarKey(int tableId, long seq) {
        return ByteBuffer.allocate(13).put(KS_QUAR).putInt(tableId).putLong(seq).array();
    }

    public static byte[] quarPrefix(int tableId) {
        return ByteBuffer.allocate(5).put(KS_QUAR).putInt(tableId).array();
    }

    // ---- business key encoding -------------------------------------------------

    private static byte tagOf(io.chronodim.api.DataType t) {
        ColumnType kind = switch (t) {
            case io.chronodim.api.DataType.Scalar s -> s.kind();
            case io.chronodim.api.DataType.Array a -> ColumnType.ARRAY;
            case io.chronodim.api.DataType.MapType m -> ColumnType.MAP;
            case io.chronodim.api.DataType.Struct s -> ColumnType.STRUCT;
        };
        return (byte) (kind.ordinal() + 1); // frozen: ordinals are append-only
    }

    /**
     * Reused per-thread scratch buffer for the three hot encoders (R-PERF-1).
     * Safe because none of them call each other; each resets before use.
     */
    private static final ThreadLocal<Buf> SCRATCH = ThreadLocal.withInitial(() -> new Buf(512));

    /** Canonical byte encoding of business key values in config order (scalars only). */
    public static byte[] encodeBusinessKey(List<Column> keyColumns, Object[] values) {
        Buf buf = SCRATCH.get();
        buf.reset();
        for (int i = 0; i < keyColumns.size(); i++) {
            Column c = keyColumns.get(i);
            Object v = values[i];
            if (v == null) throw new ValidationException("business key column '" + c.name() + "' is null");
            buf.u8(tagOf(c.type()));
            writeTyped(buf, c.type(), v);
        }
        return buf.toArray();
    }

    public static long businessKeyHash(byte[] bkBytes) {
        return XxHash64.hash(bkBytes);
    }

    /** Change-detection hash over tracked columns (§5.2). Tag+null-flag+bytes, recursively. */
    public static long attrHash(TableSchema schema, List<String> trackedColumns, java.util.Map<String, Object> row) {
        Buf buf = SCRATCH.get();
        buf.reset();
        for (String name : trackedColumns) {
            Column c = schema.column(name);
            hashTyped(buf, c.type(), row.get(name));
        }
        return XxHash64.hash(buf.array(), 0, buf.size(), 0);
    }

    private static void hashTyped(Buf buf, io.chronodim.api.DataType t, Object v) {
        buf.u8(tagOf(t));
        if (v == null) {
            buf.u8(0);
            return;
        }
        buf.u8(1);
        switch (t) {
            case io.chronodim.api.DataType.Scalar s -> writeScalar(buf, s, v);
            case io.chronodim.api.DataType.Array a -> {
                List<?> l = (List<?>) v;
                buf.u32(l.size());
                for (Object e : l) hashTyped(buf, a.element(), e);
            }
            case io.chronodim.api.DataType.MapType m -> {
                java.util.Map<?, ?> mv = (java.util.Map<?, ?>) v;
                buf.u32(mv.size());
                for (java.util.Map.Entry<?, ?> e : mv.entrySet()) {
                    hashTyped(buf, m.key(), e.getKey());
                    hashTyped(buf, m.value(), e.getValue());
                }
            }
            case io.chronodim.api.DataType.Struct st -> {
                java.util.Map<?, ?> mv = (java.util.Map<?, ?>) v;
                for (io.chronodim.api.DataType.Struct.Field f : st.fields()) {
                    hashTyped(buf, f.type(), mv.get(f.name()));
                }
            }
        }
    }

    private static void writeTyped(Buf buf, io.chronodim.api.DataType t, Object v) {
        switch (t) {
            case io.chronodim.api.DataType.Scalar s -> writeScalar(buf, s, v);
            case io.chronodim.api.DataType.Array a -> {
                List<?> l = (List<?>) v;
                buf.u32(l.size());
                for (Object e : l) {
                    if (e == null) {
                        buf.u8(0);
                    } else {
                        buf.u8(1);
                        writeTyped(buf, a.element(), e);
                    }
                }
            }
            case io.chronodim.api.DataType.MapType m -> {
                java.util.Map<?, ?> mv = (java.util.Map<?, ?>) v;
                buf.u32(mv.size());
                for (java.util.Map.Entry<?, ?> e : mv.entrySet()) {
                    writeTyped(buf, m.key(), e.getKey()); // map keys are non-null scalars
                    if (e.getValue() == null) {
                        buf.u8(0);
                    } else {
                        buf.u8(1);
                        writeTyped(buf, m.value(), e.getValue());
                    }
                }
            }
            case io.chronodim.api.DataType.Struct st -> {
                java.util.Map<?, ?> mv = (java.util.Map<?, ?>) v;
                for (io.chronodim.api.DataType.Struct.Field f : st.fields()) {
                    Object fv = mv.get(f.name());
                    if (fv == null) {
                        buf.u8(0);
                    } else {
                        buf.u8(1);
                        writeTyped(buf, f.type(), fv);
                    }
                }
            }
        }
    }

    private static void writeScalar(Buf buf, io.chronodim.api.DataType.Scalar s, Object v) {
        switch (s.kind()) {
            case STRING -> {
                byte[] b = ((String) v).getBytes(StandardCharsets.UTF_8);
                buf.u32(b.length).bytes(b);
            }
            case LONG, TIMESTAMP, TIMESTAMP_NTZ -> buf.u64((Long) v);
            case DOUBLE -> buf.u64(Double.doubleToLongBits((Double) v));
            case FLOAT -> buf.u32(Float.floatToIntBits((Float) v));
            case BOOLEAN -> buf.u8((Boolean) v ? 1 : 0);
            case DATE, INT -> buf.u32(((Number) v).intValue());
            case SMALLINT -> buf.u16(((Number) v).shortValue() & 0xFFFF);
            case TINYINT -> buf.u8(((Number) v).byteValue() & 0xFF);
            case DECIMAL -> {
                byte[] b = ((BigDecimal) v).unscaledValue().toByteArray();
                buf.u32(b.length).bytes(b);
            }
            case BYTES -> {
                byte[] b = (byte[]) v;
                buf.u32(b.length).bytes(b);
            }
            case ARRAY, MAP, STRUCT -> throw new IllegalStateException("nested kind in scalar writer");
        }
    }

    private static Object readTyped(ByteBuffer b, io.chronodim.api.DataType t) {
        return switch (t) {
            case io.chronodim.api.DataType.Scalar s -> readScalar(b, s);
            case io.chronodim.api.DataType.Array a -> {
                int n = b.getInt();
                List<Object> out = new java.util.ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    out.add(b.get() == 0 ? null : readTyped(b, a.element()));
                }
                yield out;
            }
            case io.chronodim.api.DataType.MapType m -> {
                int n = b.getInt();
                java.util.Map<Object, Object> out = new java.util.LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    Object k = readTyped(b, m.key());
                    out.put(k, b.get() == 0 ? null : readTyped(b, m.value()));
                }
                yield out;
            }
            case io.chronodim.api.DataType.Struct st -> {
                java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
                for (io.chronodim.api.DataType.Struct.Field f : st.fields()) {
                    out.put(f.name(), b.get() == 0 ? null : readTyped(b, f.type()));
                }
                yield out;
            }
        };
    }

    private static Object readScalar(ByteBuffer b, io.chronodim.api.DataType.Scalar s) {
        return switch (s.kind()) {
            case STRING -> {
                byte[] u = new byte[b.getInt()];
                b.get(u);
                yield new String(u, StandardCharsets.UTF_8);
            }
            case LONG, TIMESTAMP, TIMESTAMP_NTZ -> b.getLong();
            case DOUBLE -> Double.longBitsToDouble(b.getLong());
            case FLOAT -> Float.intBitsToFloat(b.getInt());
            case BOOLEAN -> b.get() != 0;
            case DATE, INT -> b.getInt();
            case SMALLINT -> b.getShort();
            case TINYINT -> b.get();
            case DECIMAL -> {
                byte[] u = new byte[b.getInt()];
                b.get(u);
                yield new BigDecimal(new BigInteger(u), s.scale());
            }
            case BYTES -> {
                byte[] u = new byte[b.getInt()];
                b.get(u);
                yield u;
            }
            case ARRAY, MAP, STRUCT -> throw new IllegalStateException("nested kind in scalar reader");
        };
    }

    // ---- data values -----------------------------------------------------------

    public record DecodedValue(
            byte op,
            long validTo,
            long txTime,
            long txnId,
            int schemaVersion,
            long attrHash,
            byte[] bkBytes,
            Object[] payload) {}

    public static byte[] encodeValue(Op op, long validTo, long txTime, long txnId, int schemaVersion,
                                     long attrHash, byte[] bkBytes, TableSchema schema, java.util.Map<String, Object> row) {
        Buf buf = SCRATCH.get();
        buf.reset();
        buf.u8(VALUE_VERSION).u8(op.code)
                .u64(validTo).u64(txTime).u64(txnId).u32(schemaVersion).u64(attrHash)
                .u16(bkBytes.length).bytes(bkBytes);
        for (Column c : schema.columns()) {
            Object v = row.get(c.name());
            if (v == null) {
                buf.u8(0);
            } else {
                buf.u8(1);
                writeTyped(buf, c.type(), v);
            }
        }
        return buf.toArray();
    }

    /** Decodes a value written under {@code schemaAt(schemaVersion)} of the config. */
    public static DecodedValue decodeValue(byte[] value, java.util.function.IntFunction<TableSchema> schemaLookup) {
        ByteBuffer b = ByteBuffer.wrap(value);
        byte ver = b.get();
        if (ver != VALUE_VERSION) throw new CorruptionException("unsupported value version " + ver);
        byte op = b.get();
        long validTo = b.getLong();
        long txTime = b.getLong();
        long txnId = b.getLong();
        int schemaVersion = b.getInt();
        long attrHash = b.getLong();
        int bkLen = b.getShort() & 0xFFFF;
        byte[] bk = new byte[bkLen];
        b.get(bk);
        TableSchema schema = schemaLookup.apply(schemaVersion);
        Object[] payload = new Object[schema.columns().size()];
        for (int i = 0; i < payload.length; i++) {
            if (b.get() != 0) payload[i] = readTyped(b, schema.columns().get(i).type());
        }
        return new DecodedValue(op, validTo, txTime, txnId, schemaVersion, attrHash, bk, payload);
    }

    /** Reads only the fixed header fields (flyweight path — no payload decode). */
    public static DecodedValue decodeHeader(byte[] value) {
        ByteBuffer b = ByteBuffer.wrap(value);
        byte ver = b.get();
        if (ver != VALUE_VERSION) throw new CorruptionException("unsupported value version " + ver);
        byte op = b.get();
        long validTo = b.getLong();
        long txTime = b.getLong();
        long txnId = b.getLong();
        int schemaVersion = b.getInt();
        long attrHash = b.getLong();
        int bkLen = b.getShort() & 0xFFFF;
        byte[] bk = new byte[bkLen];
        b.get(bk);
        return new DecodedValue(op, validTo, txTime, txnId, schemaVersion, attrHash, bk, null);
    }

    /**
     * Returns a copy of {@code value} with header validTo/txTime/txnId replaced —
     * used when superseding a version (the payload bytes are untouched).
     */
    public static byte[] withNewValidTo(byte[] value, long validTo, long txTime, long txnId) {
        byte[] out = value.clone();
        ByteBuffer b = ByteBuffer.wrap(out);
        b.position(2);
        b.putLong(validTo).putLong(txTime).putLong(txnId);
        return out;
    }

    /** Full header patch (op + temporal fields); the bulk-backfill path uses this. */
    public static byte[] patchHeader(byte[] value, Op op, long validTo, long txTime, long txnId) {
        byte[] out = value.clone();
        ByteBuffer b = ByteBuffer.wrap(out);
        b.position(1);
        b.put(op.code).putLong(validTo).putLong(txTime).putLong(txnId);
        return out;
    }

    // ---- growable buffer ------------------------------------------------------

    static final class Buf {
        private byte[] a;
        private int n;

        Buf(int cap) {
            a = new byte[cap];
        }

        Buf u8(int v) {
            ensure(1);
            a[n++] = (byte) v;
            return this;
        }

        Buf u16(int v) {
            ensure(2);
            a[n++] = (byte) (v >>> 8);
            a[n++] = (byte) v;
            return this;
        }

        Buf u32(int v) {
            ensure(4);
            a[n++] = (byte) (v >>> 24);
            a[n++] = (byte) (v >>> 16);
            a[n++] = (byte) (v >>> 8);
            a[n++] = (byte) v;
            return this;
        }

        Buf u64(long v) {
            ensure(8);
            for (int s = 56; s >= 0; s -= 8) a[n++] = (byte) (v >>> s);
            return this;
        }

        Buf bytes(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, a, n, b.length);
            n += b.length;
            return this;
        }

        void reset() {
            n = 0;
        }

        void ensure(int m) {
            if (n + m > a.length) a = java.util.Arrays.copyOf(a, Math.max(a.length * 2, n + m));
        }

        byte[] array() {
            return a;
        }

        int size() {
            return n;
        }

        byte[] toArray() {
            return java.util.Arrays.copyOf(a, n);
        }
    }
}
