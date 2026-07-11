package io.chronodim.api;

/**
 * A single column in a table schema. The type is a full {@link DataType} tree —
 * scalars, or arbitrarily nested array/map/struct.
 */
public record Column(String name, DataType type) {
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

    /** Convenience constructor for scalar columns (kept for existing call sites/tests). */
    public Column(String name, ColumnType kind, int precision, int scale) {
        this(name, kind == ColumnType.DECIMAL
                ? new DataType.Scalar(kind, precision, scale)
                : DataType.scalar(kind));
    }

    public String typeDeclaration() {
        return type.declaration();
    }

    /** The scalar kind, or the container kind (ARRAY/MAP/STRUCT) for nested types. */
    public ColumnType kind() {
        return switch (type) {
            case DataType.Scalar s -> s.kind();
            case DataType.Array a -> ColumnType.ARRAY;
            case DataType.MapType m -> ColumnType.MAP;
            case DataType.Struct s -> ColumnType.STRUCT;
        };
    }

    public boolean isScalar() {
        return type instanceof DataType.Scalar;
    }

    /** Decimal scale for scalar decimal columns; 0 otherwise. */
    public int scale() {
        return type instanceof DataType.Scalar s ? s.scale() : 0;
    }
}
