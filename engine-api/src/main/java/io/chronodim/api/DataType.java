package io.chronodim.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The full column type tree — every type Spark SQL can store in a table:
 * all scalar types plus arbitrarily nested {@code array<...>}, {@code map<k,v>}
 * and {@code struct<name:type, ...>}.
 *
 * <p>Canonical Java representations: array → {@link List}, map → {@link Map}
 * (insertion-ordered), struct → {@link Map} keyed by field name. Scalars as in
 * {@link ColumnType}.
 *
 * <p>Not supported (rejected at parse): interval types, variant, null type,
 * user-defined types. {@code char(n)}/{@code varchar(n)} parse as string
 * (Spark treats them as string annotations too).
 */
public sealed interface DataType permits DataType.Scalar, DataType.Array, DataType.MapType, DataType.Struct {

    /** Type declaration string, e.g. {@code array<struct<city:string,zip:string>>}. */
    String declaration();

    /** Coerces external input (JSON/CSV/user values) to the canonical representation. */
    Object coerce(Object v);

    /** Renders a canonical value to a JSON-friendly form (ISO dates, plain decimals, base64 bytes). */
    Object render(Object v);

    static DataType scalar(ColumnType kind) {
        return new Scalar(kind, 0, 0);
    }

    record Scalar(ColumnType kind, int precision, int scale) implements DataType {
        public Scalar {
            if (kind == ColumnType.ARRAY || kind == ColumnType.MAP || kind == ColumnType.STRUCT) {
                throw new ConfigException("internal: nested kind used as scalar");
            }
        }

        @Override
        public String declaration() {
            return kind == ColumnType.DECIMAL ? "decimal(" + precision + "," + scale + ")"
                    : kind.name().toLowerCase(Locale.ROOT);
        }

        @Override
        public Object coerce(Object v) {
            return kind.coerce(v, scale);
        }

        @Override
        public Object render(Object v) {
            return v == null ? null : kind.render(v);
        }
    }

    record Array(DataType element) implements DataType {
        @Override
        public String declaration() {
            return "array<" + element.declaration() + ">";
        }

        @Override
        public Object coerce(Object v) {
            if (v == null) return null;
            if (v instanceof String s) return coerce(parseJson(s, this));
            if (!(v instanceof List<?> l)) {
                throw new ValidationException("expected an array for " + declaration() + ", got " + v.getClass().getSimpleName());
            }
            List<Object> out = new ArrayList<>(l.size());
            for (Object e : l) out.add(element.coerce(e));
            return out;
        }

        @Override
        public Object render(Object v) {
            if (v == null) return null;
            List<?> l = (List<?>) v;
            List<Object> out = new ArrayList<>(l.size());
            for (Object e : l) out.add(e == null ? null : element.render(e));
            return out;
        }
    }

    record MapType(DataType key, DataType value) implements DataType {
        public MapType {
            if (!(key instanceof Scalar)) {
                throw new ConfigException("map keys must be scalar, got " + key.declaration());
            }
        }

        @Override
        public String declaration() {
            return "map<" + key.declaration() + "," + value.declaration() + ">";
        }

        @Override
        public Object coerce(Object v) {
            if (v == null) return null;
            if (v instanceof String s) return coerce(parseJson(s, this));
            if (!(v instanceof Map<?, ?> m)) {
                throw new ValidationException("expected an object for " + declaration() + ", got " + v.getClass().getSimpleName());
            }
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Object k = key.coerce(e.getKey());
                if (k == null) throw new ValidationException("map key must not be null in " + declaration());
                out.put(k, value.coerce(e.getValue()));
            }
            return out;
        }

        @Override
        public Object render(Object v) {
            if (v == null) return null;
            Map<?, ?> m = (Map<?, ?>) v;
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(String.valueOf(key.render(e.getKey())),
                        e.getValue() == null ? null : value.render(e.getValue()));
            }
            return out;
        }
    }

    record Struct(List<Field> fields) implements DataType {
        public record Field(String name, DataType type) {
            public Field {
                if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    throw new ConfigException("struct field name '" + name + "' must match [A-Za-z_][A-Za-z0-9_]*");
                }
            }
        }

        public Struct {
            if (fields == null || fields.isEmpty()) throw new ConfigException("struct must declare at least one field");
            fields = List.copyOf(fields);
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (Field f : fields) {
                if (!seen.add(f.name())) throw new ConfigException("duplicate struct field '" + f.name() + "'");
            }
        }

        @Override
        public String declaration() {
            StringBuilder sb = new StringBuilder("struct<");
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(fields.get(i).name()).append(':').append(fields.get(i).type().declaration());
            }
            return sb.append('>').toString();
        }

        @Override
        public Object coerce(Object v) {
            if (v == null) return null;
            if (v instanceof String s) return coerce(parseJson(s, this));
            if (!(v instanceof Map<?, ?> m)) {
                throw new ValidationException("expected an object for " + declaration() + ", got " + v.getClass().getSimpleName());
            }
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field f : fields) {
                out.put(f.name(), f.type().coerce(m.get(f.name())));
            }
            return out;
        }

        @Override
        public Object render(Object v) {
            if (v == null) return null;
            Map<?, ?> m = (Map<?, ?>) v;
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field f : fields) {
                Object fv = m.get(f.name());
                out.put(f.name(), fv == null ? null : f.type().render(fv));
            }
            return out;
        }
    }

    // ---- declaration parsing --------------------------------------------------------

    /** Parses a type declaration: scalar names, decimal(p,s), array&lt;..&gt;, map&lt;k,v&gt;, struct&lt;a:t,..&gt;. */
    static DataType parse(String decl) {
        Parser p = new Parser(decl);
        DataType t = p.parseType();
        p.skipWs();
        if (!p.eof()) throw new ConfigException("trailing content in type declaration '" + decl + "'");
        return t;
    }

    final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            if (s == null || s.isBlank()) throw new ConfigException("empty type declaration");
            this.s = s;
        }

        boolean eof() {
            return i >= s.length();
        }

        void skipWs() {
            while (!eof() && Character.isWhitespace(s.charAt(i))) i++;
        }

        DataType parseType() {
            skipWs();
            String name = readWord().toLowerCase(Locale.ROOT);
            skipWs();
            switch (name) {
                case "array" -> {
                    expect('<');
                    DataType el = parseType();
                    skipWs();
                    expect('>');
                    return new Array(el);
                }
                case "map" -> {
                    expect('<');
                    DataType k = parseType();
                    skipWs();
                    expect(',');
                    DataType v = parseType();
                    skipWs();
                    expect('>');
                    return new MapType(k, v);
                }
                case "struct" -> {
                    expect('<');
                    List<Struct.Field> fields = new ArrayList<>();
                    while (true) {
                        skipWs();
                        String fname = readWord();
                        skipWs();
                        expect(':');
                        fields.add(new Struct.Field(fname, parseType()));
                        skipWs();
                        char c = read();
                        if (c == '>') return new Struct(fields);
                        if (c != ',') throw err("expected ',' or '>' in struct declaration");
                    }
                }
                case "decimal", "numeric" -> {
                    expect('(');
                    int prec = readInt();
                    skipWs();
                    expect(',');
                    int scale = readInt();
                    skipWs();
                    expect(')');
                    if (prec < 1 || prec > 38 || scale < 0 || scale > prec) {
                        throw new ConfigException("decimal(" + prec + "," + scale + ") out of range (1<=p<=38, 0<=s<=p)");
                    }
                    return new Scalar(ColumnType.DECIMAL, prec, scale);
                }
                case "char", "varchar" -> {
                    // length is an annotation in Spark too; accept and treat as string
                    if (!eof() && s.charAt(i) == '(') {
                        expect('(');
                        readInt();
                        skipWs();
                        expect(')');
                    }
                    return scalar(ColumnType.STRING);
                }
                default -> {
                    ColumnType kind = switch (name) {
                        case "string", "text" -> ColumnType.STRING;
                        case "long", "bigint" -> ColumnType.LONG;
                        case "int", "integer" -> ColumnType.INT;
                        case "smallint", "short" -> ColumnType.SMALLINT;
                        case "tinyint", "byte" -> ColumnType.TINYINT;
                        case "double" -> ColumnType.DOUBLE;
                        case "float", "real" -> ColumnType.FLOAT;
                        case "boolean", "bool" -> ColumnType.BOOLEAN;
                        case "date" -> ColumnType.DATE;
                        case "timestamp" -> ColumnType.TIMESTAMP;
                        case "timestamp_ntz" -> ColumnType.TIMESTAMP_NTZ;
                        case "bytes", "binary" -> ColumnType.BYTES;
                        case "interval", "variant", "void", "null" ->
                                throw new ConfigException("type '" + name + "' is not storable"
                                        + " (Spark intervals/variant/null are not table-storage types here;"
                                        + " use string or a numeric column instead)");
                        default -> throw new ConfigException("unknown type '" + name + "'");
                    };
                    return scalar(kind);
                }
            }
        }

        private String readWord() {
            int start = i;
            while (!eof() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) i++;
            if (start == i) throw err("expected a type name");
            return s.substring(start, i);
        }

        private int readInt() {
            skipWs();
            int start = i;
            while (!eof() && Character.isDigit(s.charAt(i))) i++;
            if (start == i) throw err("expected a number");
            return Integer.parseInt(s.substring(start, i));
        }

        private char read() {
            if (eof()) throw err("unexpected end");
            return s.charAt(i++);
        }

        private void expect(char c) {
            skipWs();
            if (eof() || s.charAt(i) != c) throw err("expected '" + c + "'");
            i++;
        }

        private ConfigException err(String msg) {
            return new ConfigException("type declaration '" + s + "' at position " + i + ": " + msg);
        }
    }

    private static Object parseJson(String s, DataType target) {
        // Complex values arriving as strings (CSV cells) must be JSON.
        try {
            return MiniJson.parse(s);
        } catch (RuntimeException e) {
            throw new ValidationException("cannot parse '" + s + "' as JSON for " + target.declaration() + ": " + e.getMessage());
        }
    }
}
