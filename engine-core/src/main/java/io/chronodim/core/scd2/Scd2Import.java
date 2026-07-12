package io.chronodim.core.scd2;

import io.chronodim.api.Column;
import io.chronodim.api.ColumnType;
import io.chronodim.api.InputRow;
import io.chronodim.api.ValidationException;
import io.chronodim.core.catalog.TableCatalog.TableRuntime;
import io.chronodim.core.codec.Codecs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Imports rows that are ALREADY SCD2-shaped — carrying explicit interval columns
 * (Databricks convention {@code __START_AT}/{@code __END_AT} by default; the
 * names are configurable because every team's template differs).
 *
 * <p>Semantics per business key, rows sorted by start:
 * <ul>
 *   <li>each interval row → an upsert effective at its start (the next row's
 *       start closes it, reproducing the chain);</li>
 *   <li>a gap (row's end &lt; next row's start) → the entity was dead in between:
 *       a soft delete effective at that end is synthesized;</li>
 *   <li>a non-null end on the LAST row → the entity ended: a soft delete
 *       effective at that end is synthesized (no active row, matching
 *       "{@code __END_AT IS NULL} = active").</li>
 * </ul>
 * Rows without the start column pass through unchanged (mixed files work).
 */
public final class Scd2Import {

    public static final String DEFAULT_START = "__START_AT";
    public static final String DEFAULT_END = "__END_AT";

    private Scd2Import() {}

    public static boolean looksLikeScd2(List<InputRow> rows, String startField) {
        for (InputRow r : rows) {
            if (r.values().containsKey(startField)) return true;
        }
        return false;
    }

    public static List<InputRow> toInputRows(TableRuntime rt, List<InputRow> raw) {
        return toInputRows(rt, raw, DEFAULT_START, DEFAULT_END);
    }

    public static List<InputRow> toInputRows(TableRuntime rt, List<InputRow> raw, String startField, String endField) {
        List<Column> bkCols = rt.businessKeyColumns();
        List<InputRow> out = new ArrayList<>(raw.size());
        Map<BytesKey, List<Interval>> byKey = new LinkedHashMap<>();

        long index = -1;
        for (InputRow in : raw) {
            index++;
            if (!in.values().containsKey(startField)) {
                out.add(in); // plain row, normal upsert path
                continue;
            }
            Map<String, Object> values = new LinkedHashMap<>(in.values());
            Object startRaw = values.remove(startField);
            Object endRaw = values.remove(endField);
            long start = parseMicros(startRaw, startField, index);
            Long end = endRaw == null ? null : parseMicros(endRaw, endField, index);
            if (end != null && end <= start) {
                throw new ValidationException("row " + index + ": " + endField + " (" + endRaw
                        + ") must be after " + startField + " (" + startRaw + ")");
            }
            byte[] bk = businessKeyBytes(rt, bkCols, values, index);
            byKey.computeIfAbsent(new BytesKey(bk), k -> new ArrayList<>())
                    .add(new Interval(start, end, values, in.delete()));
        }

        for (List<Interval> intervals : byKey.values()) {
            intervals.sort(Comparator.comparingLong(i -> i.start));
            for (int i = 0; i < intervals.size(); i++) {
                Interval cur = intervals.get(i);
                out.add(new InputRow(cur.values, cur.delete, cur.start));
                boolean last = i == intervals.size() - 1;
                Long nextStart = last ? null : intervals.get(i + 1).start;
                // Synthesize the delete when the interval closed and nothing succeeds it
                // seamlessly: either a gap before the next version, or end-of-life.
                if (!cur.delete && cur.end != null && (last || cur.end < nextStart)) {
                    Map<String, Object> bkOnly = new LinkedHashMap<>();
                    for (Column c : bkCols) bkOnly.put(c.name(), cur.values.get(c.name()));
                    out.add(new InputRow(bkOnly, true, cur.end));
                }
            }
        }
        return out;
    }

    private static long parseMicros(Object v, String field, long index) {
        try {
            return switch (v) {
                case Long l -> l;
                case Number n -> n.longValue();
                case String s -> ColumnType.parseTimestampMicros(s.trim());
                case null -> throw new ValidationException("null");
                default -> throw new ValidationException("unsupported type " + v.getClass().getSimpleName());
            };
        } catch (Exception e) {
            throw new ValidationException("row " + index + ": cannot parse " + field + " value '" + v + "': " + e.getMessage());
        }
    }

    private static byte[] businessKeyBytes(TableRuntime rt, List<Column> bkCols, Map<String, Object> values, long index) {
        Object[] coerced = new Object[bkCols.size()];
        for (int i = 0; i < bkCols.size(); i++) {
            Column c = bkCols.get(i);
            Object v = values.get(c.name());
            if (v == null) {
                throw new ValidationException("row " + index + ": business key column '" + c.name() + "' missing");
            }
            coerced[i] = c.type().coerce(v);
        }
        return Codecs.encodeBusinessKey(bkCols, coerced);
    }

    private record Interval(long start, Long end, Map<String, Object> values, boolean delete) {}

    private record BytesKey(byte[] bytes) {
        @Override
        public boolean equals(Object o) {
            return o instanceof BytesKey b && Arrays.equals(bytes, b.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
