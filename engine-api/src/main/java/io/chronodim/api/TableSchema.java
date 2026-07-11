package io.chronodim.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One immutable version of a table's column list (R-CFG-4). Every stored record
 * remembers the schema version it was written with; reads up-convert to the latest.
 */
public record TableSchema(int schemaVersion, List<Column> columns) {
    public TableSchema {
        if (schemaVersion < 1) throw new ConfigException("schema_version must be >= 1");
        if (columns == null || columns.isEmpty()) throw new ConfigException("schema must declare at least one column");
        columns = List.copyOf(columns);
        Map<String, Column> seen = new HashMap<>();
        for (Column c : columns) {
            if (seen.put(c.name(), c) != null) throw new ConfigException("duplicate column '" + c.name() + "'");
        }
    }

    public Column column(String name) {
        for (Column c : columns) if (c.name().equals(name)) return c;
        return null;
    }

    public int indexOf(String name) {
        for (int i = 0; i < columns.size(); i++) if (columns.get(i).name().equals(name)) return i;
        return -1;
    }
}
