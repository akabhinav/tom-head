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

    private RowFileReader() {}

    /** table → rows. Single-table sources return one entry keyed by {@code defaultTable}. */
    static Map<String, List<InputRow>> read(Path file, String format, String defaultTable) {
        String fmt = format != null ? format.toLowerCase(Locale.ROOT) : detect(file);
        try {
            return switch (fmt) {
                case "json" -> readJson(Files.readString(file), defaultTable);
                case "jsonl", "ndjson" -> {
                    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                        yield Map.of(requireTable(defaultTable), readJsonl(r));
                    }
                }
                case "csv" -> {
                    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                        yield Map.of(requireTable(defaultTable), readCsv(r));
                    }
                }
                default -> throw new ValidationException("unsupported input format '" + fmt
                        + "' (use json, jsonl or csv, or pass --format)");
            };
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    static Map<String, List<InputRow>> readStdin(String format, String defaultTable) {
        String fmt = format == null ? "jsonl" : format.toLowerCase(Locale.ROOT);
        Reader in = new InputStreamReader(System.in, StandardCharsets.UTF_8);
        try {
            return switch (fmt) {
                case "jsonl", "ndjson" -> Map.of(requireTable(defaultTable), readJsonl(new BufferedReader(in)));
                case "csv" -> Map.of(requireTable(defaultTable), readCsv(new BufferedReader(in)));
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
    private static Map<String, List<InputRow>> readJson(String text, String defaultTable) {
        Object parsed = Json.parse(text);
        if (parsed instanceof List<?> arr) {
            List<InputRow> rows = new ArrayList<>(arr.size());
            for (Object o : arr) rows.add(toRow(asMap(o)));
            return Map.of(requireTable(defaultTable), rows);
        }
        if (parsed instanceof Map<?, ?> obj) {
            // {"table": [ {...}, ... ], "other_table": [...]} → cross-table transaction
            Map<String, List<InputRow>> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : obj.entrySet()) {
                if (!(e.getValue() instanceof List<?> arr)) {
                    throw new ValidationException("JSON object input must map table names to row arrays"
                            + " (key '" + e.getKey() + "' is not an array)");
                }
                List<InputRow> rows = new ArrayList<>(arr.size());
                for (Object o : arr) rows.add(toRow(asMap(o)));
                out.put(String.valueOf(e.getKey()), rows);
            }
            if (out.isEmpty()) throw new ValidationException("JSON input contains no rows");
            return out;
        }
        throw new ValidationException("JSON input must be an array of rows or an object of table→rows");
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
