package io.chronodim.api;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One SCD2 version of a business entity (§5.2). {@code row} contains the payload
 * columns up-converted to the table's current schema.
 *
 * <p>Times are epoch microseconds UTC. An open {@code validTo} is {@link #OPEN}
 * ({@code Long.MAX_VALUE}); it renders as {@code null} in JSON output.
 */
public record Version(
        String table,
        long validFrom,
        long validTo,
        long txTime,
        long txnId,
        Op op,
        long attrHash,
        int schemaVersion,
        boolean current,
        Map<String, Object> row) {

    public static final long OPEN = Long.MAX_VALUE;

    public Version {
        row = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(row));
    }

    public boolean validToIsOpen() {
        return validTo == OPEN;
    }

    public boolean isDeleteTombstone() {
        return op == Op.DELETE;
    }
}
