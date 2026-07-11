package io.chronodim.api;

import java.util.Map;

/** A row diverted to the per-table quarantine area (R-APPLY-5/6). */
public record QuarantineEntry(
        long id,
        String table,
        String loadId,
        long txnId,
        long quarantinedAtMicros,
        String reason,
        boolean delete,
        Map<String, Object> values) {

    public QuarantineEntry {
        values = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(values));
    }
}
