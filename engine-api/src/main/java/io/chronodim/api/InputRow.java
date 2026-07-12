package io.chronodim.api;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One incoming change row (R-APPLY-1). Values may be raw (strings/numbers from
 * JSON/CSV) — the engine coerces them to the column types. A row marked
 * {@code delete} closes the entity's current version and writes a tombstone
 * (when the table's delete mode is {@code soft}).
 *
 * <p>{@code validFromMicrosOverride}, when set, wins over the table's valid-time
 * mode — used by envelope {@code effective_at} defaults and by the SCD2 import
 * path (rows that already carry {@code __START_AT}/{@code __END_AT} intervals).
 */
public record InputRow(Map<String, Object> values, boolean delete, Long validFromMicrosOverride) {

    public InputRow {
        values = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public InputRow(Map<String, Object> values, boolean delete) {
        this(values, delete, null);
    }

    public static InputRow upsert(Map<String, Object> values) {
        return new InputRow(values, false, null);
    }

    public static InputRow delete(Map<String, Object> values) {
        return new InputRow(values, true, null);
    }

    public InputRow withValidFrom(long validFromMicros) {
        return new InputRow(values, delete, validFromMicros);
    }
}
