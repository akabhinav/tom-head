package io.chronodim.testkit;

import io.chronodim.api.ColumnType;
import io.chronodim.api.ConfigException;
import io.chronodim.api.QualityGate;
import io.chronodim.api.TableConfig;
import io.chronodim.core.catalog.TableConfigIO;
import io.chronodim.core.util.Csv;
import io.chronodim.core.util.Json;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigTest {

    @Test
    void yamlConfigRoundTripsThroughStoredJson() {
        TableConfig cfg = TableConfigIO.fromYaml("""
                table: customer
                business_key: [customer_id]
                schema:
                  - {name: customer_id, type: string}
                  - {name: name,        type: string}
                  - {name: risk_score,  type: double}
                  - {name: exposure,    type: "decimal(18,4)"}
                  - {name: updated_at,  type: timestamp}
                tracked_columns: [name, segment_missing_check_not_here, risk_score]
                """.replace(", segment_missing_check_not_here", ""));
        String json = TableConfigIO.toStoredJson(cfg, 7);
        TableConfigIO.Stored back = TableConfigIO.fromStoredJson(json);
        assertEquals(7, back.tableId());
        assertEquals(cfg.table(), back.config().table());
        assertEquals(cfg.businessKey(), back.config().businessKey());
        assertEquals(cfg.currentSchema().columns(), back.config().currentSchema().columns());
        assertEquals(TableConfigIO.configHash(cfg), TableConfigIO.configHash(back.config()));
    }

    @Test
    void configValidationErrors() {
        assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("table: t\nbusiness_key: [x]\nschema:\n  - {name: y, type: string}\n"));
        assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("table: t\nbusiness_key: [x]\nschema:\n  - {name: x, type: nonsense}\n"));
        assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("""
                table: t
                business_key: [x]
                schema:
                  - {name: x, type: string}
                valid_time:
                  mode: source_column
                  column: missing
                """));
        assertThrows(ConfigException.class, () -> TableConfigIO.fromYaml("""
                table: t
                business_key: [x]
                schema:
                  - {name: x, type: string}
                publish:
                  enabled: true
                """));
    }

    @Test
    void configHashChangesWithMeaningfulEdits() {
        String base = """
                table: t
                business_key: [x]
                schema:
                  - {name: x, type: string}
                  - {name: y, type: long}
                """;
        String h1 = TableConfigIO.configHash(TableConfigIO.fromYaml(base));
        String h2 = TableConfigIO.configHash(TableConfigIO.fromYaml(base.replace("type: long", "type: double")));
        assertNotEquals(h1, h2);
        assertEquals(h1, TableConfigIO.configHash(TableConfigIO.fromYaml(base)));
    }

    @Test
    void qualityGateGrammar() {
        assertNull(new QualityGate("c", "not_null").evaluate("x"));
        assertTrue(new QualityGate("c", "not_null").evaluate(null).contains("not_null"));
        assertNull(new QualityGate("c", "between 0 and 10").evaluate(5L));
        assertTrue(new QualityGate("c", "between 0 and 10").evaluate(11.5).contains("outside"));
        assertNull(new QualityGate("c", "matches [A-Z]{3}").evaluate("ABC"));
        assertTrue(new QualityGate("c", "matches [A-Z]{3}").evaluate("abc") != null);
        assertNull(new QualityGate("c", "in (RETAIL, SME)").evaluate("SME"));
        assertTrue(new QualityGate("c", "in (RETAIL, SME)").evaluate("CORP") != null);
        assertThrows(ConfigException.class, () -> new QualityGate("c", "between 10 and 0"));
        assertThrows(ConfigException.class, () -> new QualityGate("c", "gibberish rule"));
    }

    @Test
    void columnTypeCoercions() {
        assertEquals(42L, ColumnType.LONG.coerce("42", 0));
        assertEquals(42L, ColumnType.LONG.coerce(42, 0));
        assertEquals(1.5, ColumnType.DOUBLE.coerce("1.5", 0));
        assertEquals(true, ColumnType.BOOLEAN.coerce("yes", 0));
        assertEquals((int) java.time.LocalDate.parse("2024-03-01").toEpochDay(), ColumnType.DATE.coerce("2024-03-01", 0));
        assertEquals(1704067200000000L, ColumnType.TIMESTAMP.coerce("2024-01-01T00:00:00Z", 0));
        assertEquals(1704067200000000L, ColumnType.TIMESTAMP.coerce("2024-01-01", 0));
        assertEquals("12.3400", ((java.math.BigDecimal) ColumnType.DECIMAL.coerce("12.34", 4)).toPlainString());
        assertNull(ColumnType.LONG.coerce("", 0));
        assertThrows(io.chronodim.api.ValidationException.class, () -> ColumnType.LONG.coerce("abc", 0));
        assertThrows(io.chronodim.api.ValidationException.class, () -> ColumnType.LONG.coerce(1.5, 0));
    }

    @Test
    void jsonRoundTrip() {
        String src = "{\"a\": 1, \"b\": [true, null, \"x\\ny\", 2.5], \"c\": {\"d\": \"€ ≠ \\u0041\"}}";
        Object parsed = Json.parse(src);
        Object reparsed = Json.parse(Json.write(parsed));
        assertEquals(parsed, reparsed);
        assertEquals(Map.of("k", 9223372036854775807L), Json.parse("{\"k\": 9223372036854775807}"));
        assertThrows(io.chronodim.api.ValidationException.class, () -> Json.parse("{\"a\": }"));
        assertThrows(io.chronodim.api.ValidationException.class, () -> Json.parse("{\"a\": 1} trailing"));
    }

    @Test
    void csvParsing() {
        Csv csv = new Csv(new StringReader("id,name,notes\n1,\"Smith, John\",\"line1\nline2\"\n2,plain,\"quote\"\"d\"\n"));
        assertEquals(List.of("id", "name", "notes"), csv.nextRecord());
        assertEquals(List.of("1", "Smith, John", "line1\nline2"), csv.nextRecord());
        assertEquals(List.of("2", "plain", "quote\"d"), csv.nextRecord());
        assertNull(csv.nextRecord());
        assertEquals("\"a,b\"", Csv.escape("a,b"));
    }
}
