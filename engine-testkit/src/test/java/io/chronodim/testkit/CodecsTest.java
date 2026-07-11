package io.chronodim.testkit;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.Op;
import io.chronodim.api.TableSchema;
import io.chronodim.api.Version;
import io.chronodim.core.codec.Codecs;
import io.chronodim.storage.util.Bytes;
import io.chronodim.storage.util.XxHash64;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodecsTest {

    @Test
    void xxhash64MatchesReferenceVectors() {
        // Official XXH64 test vectors.
        assertEquals(0xEF46DB3751D8E999L, XxHash64.hash(new byte[0]));
        assertEquals(0xD24EC4F1A98C6E5BL, XxHash64.hash("a".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0x44BC2CF5AD770999L, XxHash64.hash("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void xxhash64OffsetsAndLongInputs() {
        Random rnd = new Random(7);
        byte[] data = new byte[1000];
        rnd.nextBytes(data);
        byte[] shifted = new byte[1010];
        System.arraycopy(data, 0, shifted, 10, 1000);
        assertEquals(XxHash64.hash(data, 0, 1000, 42), XxHash64.hash(shifted, 10, 1000, 42));
    }

    @Test
    void descEncodingOrdersSignedLongsNewestFirst() {
        long[] values = {Long.MIN_VALUE, -5, -1, 0, 1, 42, Long.MAX_VALUE};
        for (int i = 0; i < values.length - 1; i++) {
            assertTrue(Long.compareUnsigned(Codecs.desc(values[i]), Codecs.desc(values[i + 1])) > 0,
                    values[i] + " should encode above " + values[i + 1]);
            assertEquals(values[i], Codecs.undesc(Codecs.desc(values[i])));
        }
    }

    @Test
    void dataKeyRoundTripAndOrdering() {
        byte[] k1 = Codecs.dataKey(3, 77, 0, 100, 10);
        Codecs.DataKey d = Codecs.decodeDataKey(k1);
        assertEquals(3, d.tableId());
        assertEquals(77, d.keyHash());
        assertEquals(0, d.disambig());
        assertEquals(100, d.validFrom());
        assertEquals(10, d.txTime());

        // Newer validFrom sorts first; within a group newer txTime sorts first.
        assertTrue(Bytes.compare(Codecs.dataKey(3, 77, 0, 200, 10), Codecs.dataKey(3, 77, 0, 100, 10)) < 0);
        assertTrue(Bytes.compare(Codecs.dataKey(3, 77, 0, 100, 20), Codecs.dataKey(3, 77, 0, 100, 10)) < 0);
        // Entity prefix covers exactly its versions.
        byte[] prefix = Codecs.entityPrefix(3, 77, 0);
        assertTrue(Bytes.hasPrefix(k1, prefix));
        assertTrue(!Bytes.hasPrefix(Codecs.dataKey(3, 78, 0, 100, 10), prefix));
    }

    @Test
    void valueRoundTripAllTypes() {
        TableSchema schema = new TableSchema(1, List.of(
                new Column("s", ColumnType.STRING, 0, 0),
                new Column("l", ColumnType.LONG, 0, 0),
                new Column("d", ColumnType.DOUBLE, 0, 0),
                new Column("b", ColumnType.BOOLEAN, 0, 0),
                new Column("dt", ColumnType.DATE, 0, 0),
                new Column("ts", ColumnType.TIMESTAMP, 0, 0),
                new Column("dec", ColumnType.DECIMAL, 18, 4),
                new Column("by", ColumnType.BYTES, 0, 0)));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("s", "héllo");
        row.put("l", -42L);
        row.put("d", 3.25);
        row.put("b", true);
        row.put("dt", 19000);
        row.put("ts", 1_700_000_000_000_000L);
        row.put("dec", new BigDecimal("12345.6789"));
        row.put("by", new byte[]{1, 2, 3});

        byte[] bk = Codecs.encodeBusinessKey(List.of(schema.column("s")), new Object[]{"héllo"});
        byte[] v = Codecs.encodeValue(Op.UPDATE, Version.OPEN, 111, 222, 1, 999, bk, schema, row);
        Codecs.DecodedValue dv = Codecs.decodeValue(v, x -> schema);
        assertEquals(Op.UPDATE.code, dv.op());
        assertEquals(Version.OPEN, dv.validTo());
        assertEquals(111, dv.txTime());
        assertEquals(222, dv.txnId());
        assertEquals(999, dv.attrHash());
        assertArrayEquals(bk, dv.bkBytes());
        assertEquals("héllo", dv.payload()[0]);
        assertEquals(-42L, dv.payload()[1]);
        assertEquals(3.25, dv.payload()[2]);
        assertEquals(true, dv.payload()[3]);
        assertEquals(19000, dv.payload()[4]);
        assertEquals(1_700_000_000_000_000L, dv.payload()[5]);
        assertEquals(new BigDecimal("12345.6789"), dv.payload()[6]);
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) dv.payload()[7]);

        // Null column round trip
        row.put("s", null);
        byte[] v2 = Codecs.encodeValue(Op.INSERT, 5, 6, 7, 1, 0, bk, schema, row);
        assertNull(Codecs.decodeValue(v2, x -> schema).payload()[0]);
    }

    @Test
    void headerPatchPreservesPayload() {
        TableSchema schema = new TableSchema(1, List.of(new Column("s", ColumnType.STRING, 0, 0)));
        byte[] bk = Codecs.encodeBusinessKey(List.of(schema.column("s")), new Object[]{"k"});
        byte[] v = Codecs.encodeValue(Op.INSERT, Version.OPEN, 1, 2, 1, 3, bk, schema, Map.of("s", "payload"));
        byte[] closed = Codecs.withNewValidTo(v, 500, 10, 20);
        Codecs.DecodedValue dv = Codecs.decodeValue(closed, x -> schema);
        assertEquals(500, dv.validTo());
        assertEquals(10, dv.txTime());
        assertEquals(20, dv.txnId());
        assertEquals(Op.INSERT.code, dv.op());
        assertEquals("payload", dv.payload()[0]);

        byte[] patched = Codecs.patchHeader(v, Op.DELETE, 900, 11, 21);
        Codecs.DecodedValue dp = Codecs.decodeValue(patched, x -> schema);
        assertEquals(Op.DELETE.code, dp.op());
        assertEquals(900, dp.validTo());
    }

    @Test
    void attrHashDetectsChangesAndIgnoresUntracked() {
        TableSchema schema = new TableSchema(1, List.of(
                new Column("k", ColumnType.STRING, 0, 0),
                new Column("a", ColumnType.STRING, 0, 0),
                new Column("z", ColumnType.STRING, 0, 0)));
        Map<String, Object> r1 = Map.of("k", "x", "a", "1", "z", "ignored1");
        Map<String, Object> r2 = Map.of("k", "x", "a", "1", "z", "ignored2");
        Map<String, Object> r3 = Map.of("k", "x", "a", "2", "z", "ignored1");
        List<String> tracked = List.of("a");
        assertEquals(Codecs.attrHash(schema, tracked, r1), Codecs.attrHash(schema, tracked, r2));
        assertTrue(Codecs.attrHash(schema, tracked, r1) != Codecs.attrHash(schema, tracked, r3));
    }
}
