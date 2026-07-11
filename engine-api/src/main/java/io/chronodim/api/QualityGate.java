package io.chronodim.api;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A declarative row-level quality rule (R-APPLY-6). Rules run before any mutation.
 *
 * <p>Supported rule grammar:
 * <ul>
 *   <li>{@code not_null}</li>
 *   <li>{@code between <min> and <max>} (numeric, inclusive)</li>
 *   <li>{@code matches <regex>}</li>
 *   <li>{@code in (a, b, c)} (string comparison on the rendered value)</li>
 * </ul>
 */
public record QualityGate(String column, String rule, Check check) {

    public QualityGate(String column, String rule) {
        this(column, rule, parse(column, rule));
    }

    /** Returns null if the value passes, otherwise a human-readable failure reason. */
    public String evaluate(Object value) {
        return check.evaluate(value);
    }

    public sealed interface Check permits NotNull, Between, Matches, InSet {
        String evaluate(Object value);
    }

    public record NotNull(String column) implements Check {
        @Override public String evaluate(Object v) {
            return v == null ? "column '" + column + "' violates not_null" : null;
        }
    }

    public record Between(String column, BigDecimal min, BigDecimal max) implements Check {
        @Override public String evaluate(Object v) {
            if (v == null) return null; // null-ness is the job of not_null
            BigDecimal d;
            try {
                d = v instanceof BigDecimal b ? b : new BigDecimal(String.valueOf(v));
            } catch (NumberFormatException e) {
                return "column '" + column + "' value '" + v + "' is not numeric for rule between";
            }
            if (d.compareTo(min) < 0 || d.compareTo(max) > 0) {
                return "column '" + column + "' value " + d + " outside [" + min + ", " + max + "]";
            }
            return null;
        }
    }

    public record Matches(String column, Pattern pattern) implements Check {
        @Override public String evaluate(Object v) {
            if (v == null) return null;
            String s = String.valueOf(v);
            return pattern.matcher(s).matches() ? null
                    : "column '" + column + "' value '" + s + "' does not match /" + pattern + "/";
        }
    }

    public record InSet(String column, Set<String> allowed) implements Check {
        @Override public String evaluate(Object v) {
            if (v == null) return null;
            String s = String.valueOf(v);
            return allowed.contains(s) ? null
                    : "column '" + column + "' value '" + s + "' not in " + allowed;
        }
    }

    private static Check parse(String column, String rule) {
        if (column == null || column.isBlank()) throw new ConfigException("quality gate: column required");
        if (rule == null || rule.isBlank()) throw new ConfigException("quality gate: rule required");
        String r = rule.trim();
        String rl = r.toLowerCase(Locale.ROOT);
        if (rl.equals("not_null") || rl.equals("not null")) return new NotNull(column);
        if (rl.startsWith("between ")) {
            int andIdx = rl.indexOf(" and ");
            if (andIdx < 0) throw new ConfigException("quality gate rule '" + rule + "': expected 'between <min> and <max>'");
            try {
                BigDecimal min = new BigDecimal(r.substring(8, andIdx).trim());
                BigDecimal max = new BigDecimal(r.substring(andIdx + 5).trim());
                if (min.compareTo(max) > 0) throw new ConfigException("quality gate rule '" + rule + "': min > max");
                return new Between(column, min, max);
            } catch (NumberFormatException e) {
                throw new ConfigException("quality gate rule '" + rule + "': bounds must be numeric");
            }
        }
        if (rl.startsWith("matches ")) {
            return new Matches(column, Pattern.compile(r.substring(8).trim()));
        }
        if (rl.startsWith("in ")) {
            String body = r.substring(3).trim();
            if (!body.startsWith("(") || !body.endsWith(")")) {
                throw new ConfigException("quality gate rule '" + rule + "': expected 'in (a, b, c)'");
            }
            Set<String> vals = List.of(body.substring(1, body.length() - 1).split(",")).stream()
                    .map(String::trim)
                    .map(s -> s.length() >= 2 && (s.startsWith("'") && s.endsWith("'") || s.startsWith("\"") && s.endsWith("\""))
                            ? s.substring(1, s.length() - 1) : s)
                    .collect(Collectors.toUnmodifiableSet());
            if (vals.isEmpty()) throw new ConfigException("quality gate rule '" + rule + "': empty set");
            return new InSet(column, vals);
        }
        throw new ConfigException("quality gate rule '" + rule + "': unsupported (not_null | between .. and .. | matches .. | in (..))");
    }
}
