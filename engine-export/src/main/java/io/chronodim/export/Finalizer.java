package io.chronodim.export;

import io.chronodim.core.util.Json;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Finalizer (R-PUB-4): consolidates the append-only published parts.
 *
 * <p>Two outputs, committed atomically through the same log:
 * <ul>
 *   <li>{@code finalized/log-<seq>.jsonl} — every version record from the replaced
 *       parts, verbatim (the full bitemporal log, in fewer/larger files);</li>
 *   <li>{@code finalized/current-<seq>.jsonl} — materialized latest-knowledge SCD2
 *       shape (real {@code _valid_to}, {@code _is_current}) for consumers that
 *       don't want the view.</li>
 * </ul>
 * The SCD2 view stays correct throughout: it reads finalized log + fresh tail parts.
 */
public final class Finalizer {
    private Finalizer() {}

    public static Map<String, Object> run(Path location, List<String> businessKey) {
        return run(location, businessKey, List.of());
    }

    public static Map<String, Object> run(Path location, List<String> businessKey, List<String> partitionBy) {
        try {
            Path logDir = location.resolve(PublishedContract.LOG_DIR);
            Path dataDir = location.resolve(PublishedContract.DATA_DIR);
            Path finDir = location.resolve(PublishedContract.FINALIZED_DIR);
            Files.createDirectories(finDir);

            TreeMap<Long, Map<String, Object>> entries = new TreeMap<>();
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(logDir, "*.json")) {
                for (Path p : ds) {
                    String n = p.getFileName().toString();
                    entries.put(Long.parseLong(n.substring(0, n.length() - 5)), Json.parseObject(p == null ? "{}" : Files.readString(p)));
                }
            }
            long nextSeq = entries.isEmpty() ? 0 : entries.lastKey() + 1;
            long txnTo = -1;
            List<String> replaced = new ArrayList<>();
            List<Map<String, Object>> allRows = new ArrayList<>();

            // Collect every live file (prior finalized logs + data parts).
            for (Map<String, Object> e : entries.values()) {
                Object files = e.get("files");
                if (!(files instanceof List<?> fl)) continue;
                boolean isFinalize = "finalize".equals(e.get("type"));
                for (Object f : fl) {
                    String rel = String.valueOf(((Map<?, ?>) f).get("path"));
                    String base = rel.substring(rel.lastIndexOf('/') + 1);
                    if (isFinalize && base.startsWith("current-")) continue; // derived artifact, rebuilt below
                    Path file = location.resolve(rel);
                    if (!Files.exists(file)) continue;
                    replaced.add(rel);
                    for (String line : Files.readAllLines(file)) {
                        if (!line.isBlank()) allRows.add(Json.parseObject(line));
                    }
                }
                Object to = e.get("txn_to");
                if (to != null) txnTo = Math.max(txnTo, ((Number) to).longValue());
            }
            if (allRows.isEmpty()) {
                return Map.of("finalized_rows", 0L, "message", "nothing to finalize");
            }

            // Latest belief per (business key, _valid_from).
            Map<String, Map<String, Object>> latest = new LinkedHashMap<>();
            for (Map<String, Object> r : allRows) {
                String k = groupKey(r, businessKey) + "" + r.get(PublishedContract.VALID_FROM);
                Map<String, Object> prev = latest.get(k);
                if (prev == null || compareTx(r, prev) > 0) latest.put(k, r);
            }

            // Materialize per entity: order by _valid_from, LEAD for _valid_to.
            Map<String, List<Map<String, Object>>> byEntity = new LinkedHashMap<>();
            for (Map<String, Object> r : latest.values()) {
                byEntity.computeIfAbsent(groupKey(r, businessKey), x -> new ArrayList<>()).add(r);
            }
            List<Map<String, Object>> current = new ArrayList<>();
            for (List<Map<String, Object>> chain : byEntity.values()) {
                chain.sort(Comparator.comparing(r -> String.valueOf(r.get(PublishedContract.VALID_FROM))));
                for (int i = 0; i < chain.size(); i++) {
                    Map<String, Object> r = new LinkedHashMap<>(chain.get(i));
                    Object vt = r.get(PublishedContract.VALID_TO);
                    if (vt == null && i + 1 < chain.size()) {
                        vt = chain.get(i + 1).get(PublishedContract.VALID_FROM);
                        r.put(PublishedContract.VALID_TO, vt);
                    }
                    boolean isCurrent = vt == null && !"DELETE".equals(r.get(PublishedContract.OP));
                    r.put("_is_current", isCurrent);
                    if (isCurrent) current.add(r);
                }
            }

            // Write consolidated log (verbatim, sorted for locality) + current snapshot,
            // preserving the table's Hive-style partition layout.
            allRows.sort(Comparator
                    .comparing((Map<String, Object> r) -> groupKey(r, businessKey))
                    .thenComparing(r -> String.valueOf(r.get(PublishedContract.VALID_FROM)))
                    .thenComparing(r -> String.valueOf(r.get(PublishedContract.TX_TIME))));
            List<Map<String, Object>> fileEntries = new ArrayList<>();
            String logBase = "log-" + String.format("%020d", nextSeq) + ".jsonl";
            String curBase = "current-" + String.format("%020d", nextSeq) + ".jsonl";
            for (Map.Entry<String, List<Map<String, Object>>> pe : byPartition(allRows, partitionBy).entrySet()) {
                String rel = finalizedRel(pe.getKey(), logBase);
                writeAtomic(location.resolve(rel), pe.getValue());
                fileEntries.add(Map.of("path", rel, "rows", (long) pe.getValue().size()));
            }
            for (Map.Entry<String, List<Map<String, Object>>> pe : byPartition(current, partitionBy).entrySet()) {
                String rel = finalizedRel(pe.getKey(), curBase);
                writeAtomic(location.resolve(rel), pe.getValue());
                fileEntries.add(Map.of("path", rel, "rows", (long) pe.getValue().size()));
            }

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("seq", nextSeq);
            entry.put("type", "finalize");
            entry.put("txn_to", txnTo);
            entry.put("rows", (long) allRows.size());
            entry.put("current_rows", (long) current.size());
            entry.put("files", fileEntries);
            entry.put("replaces", replaced);
            entry.put("created_ms", System.currentTimeMillis());
            Path tmp = logDir.resolve(String.format("%020d.json.tmp", nextSeq));
            Files.writeString(tmp, Json.writePretty(entry));
            Files.move(tmp, logDir.resolve(String.format("%020d.json", nextSeq)),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            // Only now drop the replaced parts (readers of the log never saw a gap).
            for (String rel : replaced) Files.deleteIfExists(location.resolve(rel));
            // Refresh the view to read finalized log + fresh tail.
            Path view = location.resolve("scd2_view.sql");
            if (Files.exists(view)) {
                // ** matches zero or more directories, covering flat and partitioned layouts.
                Files.writeString(view, PublishedContract.viewTemplate(
                        "t",
                        "read_json_auto(['" + location.toAbsolutePath() + "/finalized/**/log-*.jsonl', '"
                                + location.toAbsolutePath() + "/data/**/*.jsonl'])",
                        businessKey));
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("finalized_rows", (long) allRows.size());
            out.put("current_rows", (long) current.size());
            out.put("replaced_files", (long) replaced.size());
            out.put("txn_to", txnTo);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("finalize failed for " + location, e);
        }
    }

    private static Map<String, List<Map<String, Object>>> byPartition(List<Map<String, Object>> rows, List<String> partitionBy) {
        Map<String, List<Map<String, Object>>> out = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            out.computeIfAbsent(PublishedContract.partitionPath(partitionBy, r), x -> new ArrayList<>()).add(r);
        }
        return out;
    }

    private static String finalizedRel(String partitionPath, String baseName) {
        return partitionPath.isEmpty()
                ? PublishedContract.FINALIZED_DIR + "/" + baseName
                : PublishedContract.FINALIZED_DIR + "/" + partitionPath + "/" + baseName;
    }

    private static void writeAtomic(Path target, List<Map<String, Object>> rows) throws IOException {
        Files.createDirectories(target.getParent());
        StringBuilder sb = new StringBuilder(rows.size() * 128);
        for (Map<String, Object> r : rows) sb.append(Json.write(r)).append('\n');
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, sb.toString());
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private static String groupKey(Map<String, Object> row, List<String> businessKey) {
        StringBuilder sb = new StringBuilder();
        for (String k : businessKey) sb.append(row.get(k)).append('');
        return sb.toString();
    }

    private static int compareTx(Map<String, Object> a, Map<String, Object> b) {
        return String.valueOf(a.get(PublishedContract.TX_TIME)).compareTo(String.valueOf(b.get(PublishedContract.TX_TIME)));
    }
}
