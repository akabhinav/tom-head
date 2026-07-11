package io.chronodim.core.util;

import io.chronodim.api.ValidationException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, strict JSON parser/writer (RFC 8259). Object → LinkedHashMap,
 * array → ArrayList, string → String, integral number → Long, decimal → Double,
 * true/false → Boolean, null → null. Dependency-free by design.
 */
public final class Json {
    private Json() {}

    // ---- parsing -------------------------------------------------------------

    public static Object parse(String text) {
        Parser p = new Parser(text);
        Object v = p.parseValue();
        p.skipWs();
        if (!p.eof()) throw p.err("trailing content after JSON value");
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) throw new ValidationException("expected JSON object, got " + typeName(v));
        return (Map<String, Object>) v;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> parseArray(String text) {
        Object v = parse(text);
        if (!(v instanceof List)) throw new ValidationException("expected JSON array, got " + typeName(v));
        return (List<Object>) v;
    }

    private static String typeName(Object v) {
        return v == null ? "null" : v.getClass().getSimpleName();
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            this.s = s;
        }

        boolean eof() {
            return i >= s.length();
        }

        Object parseValue() {
            skipWs();
            if (eof()) throw err("unexpected end of input");
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> parseObj();
                case '[' -> parseArr();
                case '"' -> parseString();
                case 't' -> lit("true", Boolean.TRUE);
                case 'f' -> lit("false", Boolean.FALSE);
                case 'n' -> lit("null", null);
                default -> parseNumber();
            };
        }

        Map<String, Object> parseObj() {
            expect('{');
            Map<String, Object> m = new LinkedHashMap<>();
            skipWs();
            if (peek() == '}') { i++; return m; }
            while (true) {
                skipWs();
                String k = parseString();
                skipWs();
                expect(':');
                m.put(k, parseValue());
                skipWs();
                char c = read();
                if (c == '}') return m;
                if (c != ',') throw err("expected ',' or '}' in object");
            }
        }

        List<Object> parseArr() {
            expect('[');
            List<Object> l = new ArrayList<>();
            skipWs();
            if (peek() == ']') { i++; return l; }
            while (true) {
                l.add(parseValue());
                skipWs();
                char c = read();
                if (c == ']') return l;
                if (c != ',') throw err("expected ',' or ']' in array");
            }
        }

        String parseString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (eof()) throw err("unterminated string");
                char c = s.charAt(i++);
                if (c == '"') return sb.toString();
                if (c == '\\') {
                    if (eof()) throw err("unterminated escape");
                    char e = s.charAt(i++);
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (i + 4 > s.length()) throw err("bad \\u escape");
                            sb.append((char) Integer.parseInt(s, i, i + 4, 16));
                            i += 4;
                        }
                        default -> throw err("bad escape '\\" + e + "'");
                    }
                } else if (c < 0x20) {
                    throw err("unescaped control character in string");
                } else {
                    sb.append(c);
                }
            }
        }

        Object parseNumber() {
            int start = i;
            if (peek() == '-') i++;
            while (!eof() && Character.isDigit(s.charAt(i))) i++;
            boolean integral = true;
            if (!eof() && s.charAt(i) == '.') {
                integral = false;
                i++;
                while (!eof() && Character.isDigit(s.charAt(i))) i++;
            }
            if (!eof() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                integral = false;
                i++;
                if (!eof() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
                while (!eof() && Character.isDigit(s.charAt(i))) i++;
            }
            String tok = s.substring(start, i);
            if (tok.isEmpty() || tok.equals("-")) throw err("invalid number");
            try {
                return integral ? (Object) Long.parseLong(tok) : (Object) Double.parseDouble(tok);
            } catch (NumberFormatException e) {
                if (integral) return Double.parseDouble(tok); // integer overflow → double
                throw err("invalid number '" + tok + "'");
            }
        }

        Object lit(String lit, Object v) {
            if (!s.startsWith(lit, i)) throw err("invalid literal");
            i += lit.length();
            return v;
        }

        void skipWs() {
            while (!eof()) {
                char c = s.charAt(i);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') i++;
                else return;
            }
        }

        char peek() {
            if (eof()) throw err("unexpected end of input");
            return s.charAt(i);
        }

        char read() {
            if (eof()) throw err("unexpected end of input");
            return s.charAt(i++);
        }

        void expect(char c) {
            if (eof() || s.charAt(i) != c) throw err("expected '" + c + "'");
            i++;
        }

        ValidationException err(String msg) {
            return new ValidationException("JSON parse error at offset " + i + ": " + msg);
        }
    }

    // ---- writing ---------------------------------------------------------------

    public static String write(Object v) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, v, -1, 0);
        return sb.toString();
    }

    public static String writePretty(Object v) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, v, 2, 0);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v, int indent, int depth) {
        switch (v) {
            case null -> sb.append("null");
            case String s -> writeString(sb, s);
            case Boolean b -> sb.append(b);
            case Double d -> {
                if (d.isNaN() || d.isInfinite()) sb.append("null");
                else sb.append(formatDouble(d));
            }
            case Float f -> writeValue(sb, f.doubleValue(), indent, depth);
            case java.math.BigDecimal d -> sb.append(d.toPlainString());
            case Number n -> sb.append(n);
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    newline(sb, indent, depth + 1);
                    writeString(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    if (indent >= 0) sb.append(' ');
                    writeValue(sb, e.getValue(), indent, depth + 1);
                }
                if (!first) newline(sb, indent, depth);
                sb.append('}');
            }
            case Iterable<?> it -> {
                sb.append('[');
                boolean first = true;
                for (Object e : it) {
                    if (!first) sb.append(',');
                    first = false;
                    newline(sb, indent, depth + 1);
                    writeValue(sb, e, indent, depth + 1);
                }
                if (!first) newline(sb, indent, depth);
                sb.append(']');
            }
            case byte[] b -> writeString(sb, java.util.Base64.getEncoder().encodeToString(b));
            default -> writeString(sb, String.valueOf(v));
        }
    }

    private static String formatDouble(double d) {
        if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            long l = (long) d;
            return l + ".0";
        }
        return Double.toString(d);
    }

    private static void newline(StringBuilder sb, int indent, int depth) {
        if (indent < 0) return;
        sb.append('\n');
        sb.append(" ".repeat(indent * depth));
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }
}
