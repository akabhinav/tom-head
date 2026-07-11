package io.chronodim.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tiny strict JSON parser used only to accept complex-typed values supplied as
 * strings (e.g. an {@code array<int>} cell in a CSV file). Objects →
 * LinkedHashMap, arrays → ArrayList, integral numbers → Long, decimals → Double.
 */
final class MiniJson {
    private final String s;
    private int i;

    private MiniJson(String s) {
        this.s = s;
    }

    static Object parse(String text) {
        MiniJson p = new MiniJson(text);
        Object v = p.value();
        p.ws();
        if (p.i < text.length()) throw new ValidationException("trailing JSON content at " + p.i);
        return v;
    }

    private Object value() {
        ws();
        if (i >= s.length()) throw new ValidationException("unexpected end of JSON");
        char c = s.charAt(i);
        return switch (c) {
            case '{' -> obj();
            case '[' -> arr();
            case '"' -> str();
            case 't' -> lit("true", Boolean.TRUE);
            case 'f' -> lit("false", Boolean.FALSE);
            case 'n' -> lit("null", null);
            default -> num();
        };
    }

    private Map<String, Object> obj() {
        i++;
        Map<String, Object> m = new LinkedHashMap<>();
        ws();
        if (peek() == '}') { i++; return m; }
        while (true) {
            ws();
            String k = str();
            ws();
            expect(':');
            m.put(k, value());
            ws();
            char c = next();
            if (c == '}') return m;
            if (c != ',') throw new ValidationException("expected ',' or '}' in JSON object");
        }
    }

    private List<Object> arr() {
        i++;
        List<Object> l = new ArrayList<>();
        ws();
        if (peek() == ']') { i++; return l; }
        while (true) {
            l.add(value());
            ws();
            char c = next();
            if (c == ']') return l;
            if (c != ',') throw new ValidationException("expected ',' or ']' in JSON array");
        }
    }

    private String str() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (i >= s.length()) throw new ValidationException("unterminated JSON string");
            char c = s.charAt(i++);
            if (c == '"') return sb.toString();
            if (c == '\\') {
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
                        sb.append((char) Integer.parseInt(s, i, i + 4, 16));
                        i += 4;
                    }
                    default -> throw new ValidationException("bad JSON escape '\\" + e + "'");
                }
            } else {
                sb.append(c);
            }
        }
    }

    private Object num() {
        int start = i;
        if (peek() == '-') i++;
        boolean integral = true;
        while (i < s.length() && (Character.isDigit(s.charAt(i)) || "+-.eE".indexOf(s.charAt(i)) >= 0)) {
            char c = s.charAt(i);
            if (c == '.' || c == 'e' || c == 'E') integral = false;
            i++;
        }
        String tok = s.substring(start, i);
        try {
            return integral ? (Object) Long.parseLong(tok) : (Object) Double.parseDouble(tok);
        } catch (NumberFormatException e) {
            throw new ValidationException("invalid JSON number '" + tok + "'");
        }
    }

    private Object lit(String lit, Object v) {
        if (!s.startsWith(lit, i)) throw new ValidationException("invalid JSON literal at " + i);
        i += lit.length();
        return v;
    }

    private void ws() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    private char peek() {
        if (i >= s.length()) throw new ValidationException("unexpected end of JSON");
        return s.charAt(i);
    }

    private char next() {
        if (i >= s.length()) throw new ValidationException("unexpected end of JSON");
        return s.charAt(i++);
    }

    private void expect(char c) {
        if (i >= s.length() || s.charAt(i) != c) throw new ValidationException("expected '" + c + "' in JSON at " + i);
        i++;
    }
}
