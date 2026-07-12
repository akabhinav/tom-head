package io.chronodim.cli;

import io.chronodim.api.InputRow;
import io.chronodim.api.ValidationException;
import io.chronodim.core.util.Csv;
import io.chronodim.core.util.Json;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Reads change rows from user files: a single JSON file (array of objects, or an
 * object mapping table → rows for cross-table transactions), JSON Lines, or CSV
 * with a header row. A {@code _op} field/column marks deletes
 * ({@code delete | d | remove}); everything else is an upsert.
 */
final class RowFileReader {

    static final String OP_FIELD = "_op";

    /** Envelope keys with engine meaning; everything else at the top level is audit metadata. */
    private static final java.util.Set<String> ENVELOPE_KEYS =
            java.util.Set.of("table", "load_id", "effective_at", "defaults", "metadata", "records");

    private RowFileReader() {}

    /**
     * Parsed input: rows per table, plus — when the source was a metadata envelope —
     * the load id, the default effective time, and free-form audit metadata.
     */
    record ParsedInput(Map<String, List<InputRow>> byTable, String loadId, String effectiveAt,
                       Map<String, Object> metadata) {
        static ParsedInput plain(Map<String, List<InputRow>> byTable) {
            return new ParsedInput(byTable, null, null, Map.of());
        }
    }

    static ParsedInput read(Path file, String format, String defaultTable) {
        String fmt = format != null ? format.toLowerCase(Locale.ROOT) : detect(file);
        try {
            return switch (fmt) {
                case "json" -> readJson(Files.readString(file), defaultTable);
                case "jsonl", "ndjson" -> {
                    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                        yield ParsedInput.plain(Map.of(requireTable(defaultTable), readJsonl(r)));
                    }
                }
                case "csv" -> {
                    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                        yield ParsedInput.plain(Map.of(requireTable(defaultTable), readCsv(r)));
                    }
                }
                default -> throw new ValidationException("unsupported input format '" + fmt
                        + "' (use json, jsonl or csv, or pass --format)");
            };
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    /** Same parsing as {@link #read}, from an in-memory body (used by the HTTP UI). */
    static ParsedInput readString(String text, String format, String defaultTable) {
        String fmt = format == null ? "json" : format.toLowerCase(Locale.ROOT);
        try {
            return switch (fmt) {
                case "json" -> readJson(text, defaultTable);
                case "jsonl", "ndjson" -> ParsedInput.plain(Map.of(requireTable(defaultTable),
                        readJsonl(new BufferedReader(new java.io.StringReader(text)))));
                case "csv" -> ParsedInput.plain(Map.of(requireTable(defaultTable),
                        readCsv(new java.io.StringReader(text))));
                default -> throw new ValidationException("unsupported input format '" + fmt + "' (json, jsonl or csv)");
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ParsedInput readStdin(String format, String defaultTable) {
        String fmt = format == null ? "jsonl" : format.toLowerCase(Locale.ROOT);
        Reader in = new InputStreamReader(System.in, StandardCharsets.UTF_8);
        try {
            return switch (fmt) {
                case "jsonl", "ndjson" -> ParsedInput.plain(Map.of(requireTable(defaultTable), readJsonl(new BufferedReader(in))));
                case "csv" -> ParsedInput.plain(Map.of(requireTable(defaultTable), readCsv(new BufferedReader(in))));
                case "json" -> {
                    StringBuilder sb = new StringBuilder();
                    char[] buf = new char[8192];
                    int n;
                    while ((n = in.read(buf)) > 0) sb.append(buf, 0, n);
                    yield readJson(sb.toString(), defaultTable);
                }
                default -> throw new ValidationException("unsupported stdin format '" + fmt + "'");
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String detect(Path file) {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) return "csv";
        if (name.endsWith(".jsonl") || name.endsWith(".ndjson")) return "jsonl";
        if (name.endsWith(".json")) return "json";
        throw new ValidationException("cannot detect format of '" + file + "' — pass --format json|jsonl|csv");
    }

    @SuppressWarnings("unchecked")
    private static ParsedInput readJson(String text, String defaultTable) {
        Object parsed = Json.parse(text);
        if (parsed instanceof List<?> arr) {
            List<InputRow> rows = new ArrayList<>(arr.size());
            for (Object o : arr) rows.add(toRow(asMap(o)));
            return ParsedInput.plain(Map.of(requireTable(defaultTable), rows));
        }
        if (parsed instanceof Map<?, ?> obj) {
            Map<String, Object> m = (Map<String, Object>) obj;
            if (m.get("records") instanceof List<?> records) {
                return readEnvelope(m, records, defaultTable);
            }
            // {"table": [ {...}, ... ], "other_table": [...]} → cross-table transaction
            Map<String, List<InputRow>> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : obj.entrySet()) {
                if (!(e.getValue() instanceof List<?> arr)) {
                    throw new ValidationException("JSON object input must map table names to row arrays"
                            + " (key '" + e.getKey() + "' is not an array; for a metadata envelope use a 'records' array)");
                }
                List<InputRow> rows = new ArrayList<>(arr.size());
                for (Object o : arr) rows.add(toRow(asMap(o)));
                out.put(String.valueOf(e.getKey()), rows);
            }
            if (out.isEmpty()) throw new ValidationException("JSON input contains no rows");
            return ParsedInput.plain(out);
        }
        throw new ValidationException("JSON input must be an array of rows, an object of table→rows,"
                + " or a metadata envelope with a 'records' array");
    }

    /**
     * Metadata envelope: {@code {table, load_id, effective_at, defaults, metadata, records}}.
     * Unrecognised top-level keys are treated as audit metadata too — teams put
     * approver/reason/source columns wherever their template says.
     */
    @SuppressWarnings("unchecked")
    private static ParsedInput readEnvelope(Map<String, Object> env, List<?> records, String defaultTable) {
        String table = env.get("table") != null ? String.valueOf(env.get("table")) : defaultTable;
        String loadId = env.get("load_id") == null ? null : String.valueOf(env.get("load_id"));
        String effectiveAt = env.get("effective_at") == null ? null : String.valueOf(env.get("effective_at"));
        Map<String, Object> defaults = env.get("defaults") == null
                ? Map.of() : (Map<String, Object>) env.get("defaults");

        Map<String, Object> metadata = new LinkedHashMap<>();
        if (env.get("metadata") instanceof Map<?, ?> mm) {
            mm.forEach((k, v) -> metadata.put(String.valueOf(k), v));
        }
        for (Map.Entry<String, Object> e : env.entrySet()) {
            if (!ENVELOPE_KEYS.contains(e.getKey())) metadata.put(e.getKey(), e.getValue());
        }

        List<InputRow> rows = new ArrayList<>(records.size());
        for (Object o : records) {
            Map<String, Object> merged = new LinkedHashMap<>(defaults);
            merged.putAll(asMap(o));
            rows.add(toRow(merged));
        }
        return new ParsedInput(Map.of(requireTable(table), rows), loadId, effectiveAt, metadata);
    }

    private static List<InputRow> readJsonl(BufferedReader r) throws IOException {
        List<InputRow> rows = new ArrayList<>();
        String line;
        long no = 0;
        while ((line = r.readLine()) != null) {
            no++;
            if (line.isBlank()) continue;
            try {
                rows.add(toRow(Json.parseObject(line)));
            } catch (ValidationException e) {
                throw new ValidationException("line " + no + ": " + e.getMessage());
            }
        }
        return rows;
    }

    private static List<InputRow> readCsv(Reader reader) {
        Csv csv = new Csv(reader);
        List<String> header = csv.nextRecord();
        if (header == null) throw new ValidationException("CSV input is empty (a header row is required)");
        List<InputRow> rows = new ArrayList<>();
        List<String> rec;
        long no = 1;
        while ((rec = csv.nextRecord()) != null) {
            no++;
            if (rec.size() != header.size()) {
                throw new ValidationException("CSV line " + no + ": " + rec.size() + " fields, header has " + header.size());
            }
            Map<String, Object> m = new LinkedHashMap<>();
            for (int i = 0; i < header.size(); i++) {
                String cell = rec.get(i);
                // CSV convention: an empty unquoted cell is NULL, not empty string.
                m.put(header.get(i).strip(), cell.isEmpty() ? null : cell);
            }
            rows.add(toRow(m));
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        if (!(o instanceof Map)) throw new ValidationException("row is not a JSON object: " + o);
        return (Map<String, Object>) o;
    }

    static InputRow toRow(Map<String, Object> raw) {
        Object op = raw.get(OP_FIELD);
        Map<String, Object> values = new LinkedHashMap<>(raw);
        values.remove(OP_FIELD);
        boolean delete = false;
        if (op != null) {
            String s = String.valueOf(op).strip().toLowerCase(Locale.ROOT);
            delete = switch (s) {
                case "delete", "d", "remove", "del" -> true;
                case "", "upsert", "u", "insert", "i", "update" -> false;
                default -> throw new ValidationException("unknown " + OP_FIELD + " value '" + op + "'");
            };
        }
        return new InputRow(values, delete);
    }

    private static String requireTable(String table) {
        if (table == null || table.isBlank()) {
            throw new ValidationException("--table is required for this input format"
                    + " (or use a JSON object mapping table names to row arrays)");
        }
        return table;
    }
}
