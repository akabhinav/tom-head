package io.chronodim.api;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One incoming change row (R-APPLY-1). Values may be raw (strings/numbers from
 * JSON/CSV) — the engine coerces them to the column types. A row marked
 * {@code delete} closes the entity's current version and writes a tombstone
 * (when the table's delete mode is {@code soft}).
 */
public record InputRow(Map<String, Object> values, boolean delete) {

    public InputRow {
        values = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public static InputRow upsert(Map<String, Object> values) {
        return new InputRow(values, false);
    }

    public static InputRow delete(Map<String, Object> values) {
        return new InputRow(values, true);
    }
}
