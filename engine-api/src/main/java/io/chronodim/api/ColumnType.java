package io.chronodim.api;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Locale;

/**
 * Column types supported by ChronoDim v1 (R-CFG-2).
 *
 * <p>Canonical Java representations used throughout the public API:
 * <ul>
 *   <li>{@code STRING}    → {@link String}</li>
 *   <li>{@code LONG}      → {@link Long}</li>
 *   <li>{@code DOUBLE}    → {@link Double}</li>
 *   <li>{@code BOOLEAN}   → {@link Boolean}</li>
 *   <li>{@code DATE}      → {@link Integer} (days since 1970-01-01, UTC)</li>
 *   <li>{@code TIMESTAMP} → {@link Long} (microseconds since epoch, UTC)</li>
 *   <li>{@code DECIMAL}   → {@link BigDecimal} (scale fixed by the column)</li>
 *   <li>{@code BYTES}     → {@code byte[]}</li>
 * </ul>
 */
public enum ColumnType {
    // Frozen on-disk type tags derive from ordinals — append only, never reorder.
    STRING, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP, DECIMAL, BYTES,
    INT, SMALLINT, TINYINT, FLOAT, TIMESTAMP_NTZ,
    ARRAY, MAP, STRUCT;

    /** True for the container kinds whose semantics live in {@link DataType}. */
    public boolean isNested() {
        return this == ARRAY || this == MAP || this == STRUCT;
    }

    /** Parses a type declaration such as {@code string}, {@code decimal(18,4)} or {@code array<int>}. */
    public static Column parseDeclaration(String name, String decl) {
        try {
            return new Column(name, DataType.parse(decl));
        } catch (ConfigException e) {
            throw new ConfigException("column '" + name + "': " + e.getMessage());
        }
    }

    /**
     * Coerces an externally supplied value (JSON, CSV, user code) into the canonical
     * representation for this type. Returns {@code null} for null/empty input.
     *
     * @throws ValidationException if the value cannot be represented in this type.
     */
    public Object coerce(Object v, int scale) {
        if (v == null) return null;
        try {
            return switch (this) {
                case STRING -> v instanceof String s ? s : String.valueOf(v);
                case LONG -> switch (v) {
                    case Long l -> l;
                    case Integer i -> i.longValue();
                    case Number n -> exactLong(n);
                    case String s -> s.isEmpty() ? null : Long.valueOf(Long.parseLong(s.trim()));
                    default -> throw bad(v);
                };
                case DOUBLE -> switch (v) {
                    case Double d -> d;
                    case Number n -> n.doubleValue();
                    case String s -> s.isEmpty() ? null : Double.valueOf(Double.parseDouble(s.trim()));
                    default -> throw bad(v);
                };
                case BOOLEAN -> switch (v) {
                    case Boolean b -> b;
                    case String s -> s.isEmpty() ? null : parseBool(s.trim());
                    default -> throw bad(v);
                };
                case DATE -> switch (v) {
                    case Integer i -> i;
                    case Number n -> (int) exactLong(n).longValue();
                    case String s -> s.isEmpty() ? null : Integer.valueOf((int) LocalDate.parse(s.trim()).toEpochDay());
                    default -> throw bad(v);
                };
                case TIMESTAMP -> switch (v) {
                    case Long l -> l;
                    case Number n -> exactLong(n);
                    case String s -> s.isEmpty() ? null : Long.valueOf(parseTimestampMicros(s.trim()));
                    default -> throw bad(v);
                };
                case DECIMAL -> switch (v) {
                    case BigDecimal b -> b.setScale(scale);
                    case Number n -> new BigDecimal(n.toString()).setScale(scale);
                    case String s -> s.isEmpty() ? null : new BigDecimal(s.trim()).setScale(scale);
                    default -> throw bad(v);
                };
                case BYTES -> switch (v) {
                    case byte[] b -> b;
                    case String s -> s.isEmpty() ? null : Base64.getDecoder().decode(s.trim());
                    default -> throw bad(v);
                };
                case INT -> intLike(v, Integer.MIN_VALUE, Integer.MAX_VALUE, Long::intValue);
                case SMALLINT -> intLike(v, Short.MIN_VALUE, Short.MAX_VALUE, l -> (short) l.longValue());
                case TINYINT -> intLike(v, Byte.MIN_VALUE, Byte.MAX_VALUE, l -> (byte) l.longValue());
                case FLOAT -> switch (v) {
                    case Float f -> f;
                    case Number n -> (float) n.doubleValue();
                    case String s -> s.isEmpty() ? null : Float.valueOf(Float.parseFloat(s.trim()));
                    default -> throw bad(v);
                };
                case TIMESTAMP_NTZ -> switch (v) {
                    case Long l -> l;
                    case Number n -> exactLong(n);
                    case String s -> s.isEmpty() ? null : Long.valueOf(parseTimestampMicros(s.trim()));
                    default -> throw bad(v);
                };
                case ARRAY, MAP, STRUCT ->
                        throw new ValidationException("internal: nested type coerced without DataType context");
            };
        } catch (ValidationException e) {
            throw e;
        } catch (DateTimeParseException | ArithmeticException | IllegalArgumentException e) {
            throw new ValidationException("cannot coerce value '" + v + "' to " + this + ": " + e.getMessage());
        }
    }

    /** Renders a canonical value back to a human/JSON-friendly form. */
    public Object render(Object v) {
        if (v == null) return null;
        return switch (this) {
            case DATE -> LocalDate.ofEpochDay(((Integer) v).longValue()).toString();
            case TIMESTAMP -> formatTimestampMicros((Long) v);
            case TIMESTAMP_NTZ -> formatTimestampNtzMicros((Long) v);
            case DECIMAL -> ((BigDecimal) v).toPlainString();
            case BYTES -> Base64.getEncoder().encodeToString((byte[]) v);
            default -> v;
        };
    }

    private static Number intLike(Object v, long min, long max, java.util.function.Function<Long, Number> narrow) {
        Long l = switch (v) {
            case Number n -> exactLong(n);
            case String s -> s.isEmpty() ? null : Long.valueOf(Long.parseLong(s.trim()));
            default -> throw new ValidationException("value of class " + v.getClass().getName() + " not valid for integer type");
        };
        if (l == null) return null;
        if (l < min || l > max) throw new ValidationException("value " + l + " outside [" + min + ", " + max + "]");
        return narrow.apply(l); // canonical form is the exact-width boxed type
    }

    /** Parses an ISO-8601 instant / offset datetime / local date, or a raw epoch-micros long. */
    public static long parseTimestampMicros(String s) {
        if (s.indexOf('T') < 0 && s.indexOf('-') < 0 && s.indexOf(':') < 0) {
            return Long.parseLong(s); // raw epoch micros
        }
        Instant i;
        try {
            i = Instant.parse(s);
        } catch (DateTimeParseException e1) {
            try {
                i = OffsetDateTime.parse(s).toInstant();
            } catch (DateTimeParseException e2) {
                try {
                    i = java.time.LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException e3) {
                    i = LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant();
                }
            }
        }
        return Math.addExact(Math.multiplyExact(i.getEpochSecond(), 1_000_000L), i.getNano() / 1_000L);
    }

    public static String formatTimestampMicros(long micros) {
        long secs = Math.floorDiv(micros, 1_000_000L);
        long rem = Math.floorMod(micros, 1_000_000L);
        return Instant.ofEpochSecond(secs, rem * 1_000L).toString();
    }

    /** NTZ rendering: local ISO-8601 without a zone suffix (matches Spark TIMESTAMP_NTZ). */
    public static String formatTimestampNtzMicros(long micros) {
        long secs = Math.floorDiv(micros, 1_000_000L);
        long rem = Math.floorMod(micros, 1_000_000L);
        return java.time.LocalDateTime.ofEpochSecond(secs, (int) (rem * 1_000L), ZoneOffset.UTC).toString();
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Long exactLong(Number n) {
        if (n instanceof Double d) {
            long l = d.longValue();
            if ((double) l != d) throw new ValidationException("value " + d + " is not an exact integer");
            return l;
        }
        return n.longValue();
    }

    private static Boolean parseBool(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "y" -> Boolean.TRUE;
            case "false", "0", "no", "n" -> Boolean.FALSE;
            default -> throw new ValidationException("cannot parse boolean '" + s + "'");
        };
    }

    private ValidationException bad(Object v) {
        return new ValidationException("value of class " + v.getClass().getName() + " not valid for type " + this);
    }
}
