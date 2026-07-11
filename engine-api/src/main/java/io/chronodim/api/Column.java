package io.chronodim.api;

/**
 * A single column in a table schema. {@code precision}/{@code scale} apply to DECIMAL only.
 */
public record Column(String name, ColumnType type, int precision, int scale) {
    public Column {
        if (name == null || name.isBlank()) throw new ConfigException("column name must be non-empty");
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new ConfigException("column name '" + name + "' must match [A-Za-z_][A-Za-z0-9_]*");
        }
        if (name.startsWith("_")) {
            throw new ConfigException("column name '" + name + "' may not start with '_' (reserved for system columns)");
        }
        if (type == null) throw new ConfigException("column '" + name + "': type required");
    }

    public String typeDeclaration() {
        return type == ColumnType.DECIMAL ? "decimal(" + precision + "," + scale + ")" : type.name().toLowerCase(java.util.Locale.ROOT);
    }
}
