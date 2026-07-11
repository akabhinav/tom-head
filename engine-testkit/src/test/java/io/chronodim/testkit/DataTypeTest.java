package io.chronodim.testkit;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.DataType;
import io.chronodim.api.Op;
import io.chronodim.api.TableSchema;
import io.chronodim.api.ValidationException;
import io.chronodim.api.Version;
import io.chronodim.core.codec.Codecs;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Spark type-surface coverage: new scalars + nested array/map/struct, end to end through the codec. */
class DataTypeTest {

    @Test
    void declarationParseRoundTrip() {
        for (String decl : List.of(
                "string", "bigint", "int", "smallint", "tinyint", "float", "double", "boolean",
                "date", "timestamp", "timestamp_ntz", "binary", "decimal(18,4)",
                "array<int>", "map<string,double>", "struct<a:int,b:string>",
                "array<struct<city:string,zip:string>>",
                "map<string,array<decimal(10,2)>>",
                "struct<inner:struct<deep:array<map<string,long>>>,flag:boolean>")) {
            DataType t = DataType.parse(decl);
            assertEquals(t, DataType.parse(t.declaration()), decl);
        }
        // aliases normalize
        assertEquals("string", DataType.parse("varchar(20)").declaration());
        assertEquals("string", DataType.parse("char(3)").declaration());
        assertEquals("long", DataType.parse("bigint").declaration());
        assertEquals("int", DataType.parse("integer").declaration());
    }

    @Test
    void unsupportedTypesRejectedClearly() {
        for (String bad : List.of("interval", "variant", "void", "wibble", "map<array<int>,string>", "struct<>")) {
            assertThrows(ConfigException.class, () -> DataType.parse(bad), bad);
        }
    }

    @Test
    void scalarCoercionsAndBounds() {
        assertEquals(42, DataType.parse("int").coerce("42"));
        assertEquals((short) 7, DataType.parse("smallint").coerce(7));
        assertEquals((byte) -3, DataType.parse("tinyint").coerce("-3"));
        assertEquals(1.5f, DataType.parse("float").coerce("1.5"));
        assertThrows(ValidationException.class, () -> DataType.parse("tinyint").coerce(300));
        assertThrows(ValidationException.class, () -> DataType.parse("smallint").coerce(70000));
        // NTZ accepts local ISO and renders without a zone
        Object ntz = DataType.parse("timestamp_ntz").coerce("2026-01-01T10:30:00");
        assertEquals("2026-01-01T10:30", DataType.parse("timestamp_ntz").render(ntz));
    }

    @Test
    void nestedCoercionFromObjectsAndJsonStrings() {
        DataType arr = DataType.parse("array<int>");
        assertEquals(List.of(1, 2, 3), arr.coerce(List.of("1", 2L, 3)));
        assertEquals(List.of(1, 2), arr.coerce("[1, 2]")); // CSV cell form

        DataType m = DataType.parse("map<string,long>");
        assertEquals(Map.of("a", 1L), m.coerce(Map.of("a", "1")));
        assertEquals(Map.of("a", 5L), m.coerce("{\"a\": 5}"));

        DataType st = DataType.parse("struct<city:string,zip:string>");
        Map<String, Object> coerced = asMap(st.coerce(Map.of("city", "Zurich", "zip", 8001)));
        assertEquals("Zurich", coerced.get("city"));
        assertEquals("8001", coerced.get("zip"));
        assertNull(asMap(st.coerce(Map.of("city", "X"))).get("zip")); // missing field → null

        assertThrows(ValidationException.class, () -> arr.coerce("not json"));
        assertThrows(ValidationException.class, () -> m.coerce(List.of(1)));
    }

    @Test
    void nestedValuesRoundTripThroughStorageCodec() {
        TableSchema schema = new TableSchema(1, List.of(
                new Column("k", DataType.parse("string")),
                new Column("i", DataType.parse("int")),
                new Column("sm", DataType.parse("smallint")),
                new Column("ty", DataType.parse("tinyint")),
                new Column("f", DataType.parse("float")),
                new Column("ntz", DataType.parse("timestamp_ntz")),
                new Column("tags", DataType.parse("array<string>")),
                new Column("addr", DataType.parse("struct<city:string,zip:string>")),
                new Column("lim", DataType.parse("map<string,long>")),
                new Column("deep", DataType.parse("array<struct<n:int,vals:array<double>>>"))));

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("k", "key1");
        row.put("i", schema.column("i").type().coerce(123456));
        row.put("sm", schema.column("sm").type().coerce(-42));
        row.put("ty", schema.column("ty").type().coerce(7));
        row.put("f", schema.column("f").type().coerce(2.5));
        row.put("ntz", schema.column("ntz").type().coerce("2026-06-01T12:00:00"));
        row.put("tags", schema.column("tags").type().coerce(List.of("a", "b")));
        row.put("addr", schema.column("addr").type().coerce(Map.of("city", "Basel", "zip", "4051")));
        row.put("lim", schema.column("lim").type().coerce(Map.of("daily", 100, "monthly", 5000)));
        row.put("deep", schema.column("deep").type().coerce(
                List.of(Map.of("n", 1, "vals", List.of(0.5, 1.5)), Map.of("n", 2, "vals", List.of()))));

        byte[] bk = Codecs.encodeBusinessKey(List.of(schema.column("k")), new Object[]{"key1"});
        byte[] value = Codecs.encodeValue(Op.INSERT, Version.OPEN, 1, 2, 1, 99, bk, schema, row);
        Codecs.DecodedValue dv = Codecs.decodeValue(value, x -> schema);

        assertEquals(123456, dv.payload()[schema.indexOf("i")]);
        assertEquals((short) -42, dv.payload()[schema.indexOf("sm")]);
        assertEquals((byte) 7, dv.payload()[schema.indexOf("ty")]);
        assertEquals(2.5f, dv.payload()[schema.indexOf("f")]);
        assertEquals(List.of("a", "b"), dv.payload()[schema.indexOf("tags")]);
        assertEquals(Map.of("city", "Basel", "zip", "4051"), dv.payload()[schema.indexOf("addr")]);
        assertEquals(row.get("lim"), dv.payload()[schema.indexOf("lim")]);
        assertEquals(row.get("deep"), dv.payload()[schema.indexOf("deep")]);
    }

    @Test
    void attrHashSeesNestedChanges() {
        TableSchema schema = new TableSchema(1, List.of(
                new Column("k", DataType.parse("string")),
                new Column("addr", DataType.parse("struct<city:string,zip:string>"))));
        List<String> tracked = List.of("addr");
        DataType st = schema.column("addr").type();
        Map<String, Object> a = Map.of("k", "x", "addr", st.coerce(Map.of("city", "Bern", "zip", "3000")));
        Map<String, Object> b = Map.of("k", "x", "addr", st.coerce(Map.of("city", "Bern", "zip", "3000")));
        Map<String, Object> c = Map.of("k", "x", "addr", st.coerce(Map.of("city", "Bern", "zip", "3001")));
        assertEquals(Codecs.attrHash(schema, tracked, a), Codecs.attrHash(schema, tracked, b));
        assertTrue(Codecs.attrHash(schema, tracked, a) != Codecs.attrHash(schema, tracked, c));
    }

    @Test
    void businessKeyAndPartitionColumnsMustBeScalar() {
        assertThrows(ConfigException.class, () -> io.chronodim.core.catalog.TableConfigIO.fromYaml("""
                table: t
                business_key: [tags]
                schema:
                  - {name: tags, type: "array<string>"}
                  - {name: v,    type: string}
                """));
        assertThrows(ConfigException.class, () -> io.chronodim.core.catalog.TableConfigIO.fromYaml("""
                table: t
                business_key: [id]
                schema:
                  - {name: id,   type: string}
                  - {name: tags, type: "array<string>"}
                publish:
                  enabled: true
                  location: /tmp/x
                  partition_by: [tags]
                """));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }
}
