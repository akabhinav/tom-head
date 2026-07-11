package io.chronodim.durability;

import io.chronodim.api.ChronoDimException;
import io.chronodim.core.util.Json;
import io.chronodim.storage.util.XxHash64;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Snapshot upload + restore (R-DUR-2/3).
 *
 * <p>Snapshot = zip of a storage checkpoint + manifest
 * {@code snapshots/snap-<txn>.json} with {@code {txn_id, files:{name:xxhash64}, created_ms}}.
 * Restore = pick the newest applicable snapshot, unzip into {@code <target>/storage},
 * verify every checksum, download shipped WAL (chunks concatenated), optionally trim
 * records beyond {@code --as-of-txn}; the engine's normal recovery replays the rest.
 */
public final class SnapshotManager {
    private SnapshotManager() {}

    /** Zips a checkpoint dir and uploads it with its manifest. Returns manifest data. */
    public static Map<String, Object> upload(ObjectStore store, Path checkpointDir, long txnId) {
        try {
            Path zip = Files.createTempFile(checkpointDir.getParent(), "snap-", ".zip");
            Map<String, Object> checksums = new LinkedHashMap<>();
            try (ZipOutputStream zout = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(zip), 1 << 20))) {
                zout.setLevel(1); // storage files are already compact; favor speed
                try (Stream<Path> walk = Files.walk(checkpointDir)) {
                    for (Path p : walk.toList()) {
                        if (Files.isDirectory(p)) continue;
                        String name = checkpointDir.relativize(p).toString().replace('\\', '/');
                        zout.putNextEntry(new ZipEntry(name));
                        byte[] content = Files.readAllBytes(p);
                        zout.write(content);
                        zout.closeEntry();
                        checksums.put(name, String.format("%016x", XxHash64.hash(content)));
                    }
                }
            }
            String base = "snapshots/snap-" + String.format("%020d", txnId);
            store.put(base + ".zip", zip);
            Map<String, Object> manifest = new LinkedHashMap<>();
            manifest.put("format_version", 1L);
            manifest.put("txn_id", txnId);
            manifest.put("files", checksums);
            manifest.put("zip_bytes", Files.size(zip));
            manifest.put("created_ms", System.currentTimeMillis());
            store.putBytes(base + ".json", Json.writePretty(manifest).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Files.deleteIfExists(zip);
            manifest.put("key", base + ".zip");
            return manifest;
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot upload failed", e);
        }
    }

    /** Restores into targetDir (must not contain a database). Returns a report. */
    public static Map<String, Object> restore(ObjectStore store, Path targetDir, Long asOfTxn) {
        try {
            if (Files.exists(targetDir.resolve("storage")) || Files.exists(targetDir.resolve("wal"))) {
                throw new ChronoDimException("restore target already contains a database: " + targetDir);
            }
            // Pick the newest snapshot at or below asOfTxn (if any exist).
            List<String> manifests = store.list("snapshots/").stream().filter(k -> k.endsWith(".json")).toList();
            Map<String, Object> chosen = null;
            long chosenTxn = -1;
            for (String key : manifests) {
                Map<String, Object> m;
                try (InputStream in = store.get(key)) {
                    m = Json.parseObject(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
                }
                long txn = ((Number) m.get("txn_id")).longValue();
                if (asOfTxn != null && txn > asOfTxn) continue;
                if (txn > chosenTxn) {
                    chosenTxn = txn;
                    chosen = m;
                }
            }

            Files.createDirectories(targetDir);
            long verified = 0;
            if (chosen != null) {
                String zipKey = "snapshots/snap-" + String.format("%020d", chosenTxn) + ".zip";
                Path storageDir = targetDir.resolve("storage");
                Files.createDirectories(storageDir);
                @SuppressWarnings("unchecked")
                Map<String, Object> checksums = (Map<String, Object>) chosen.get("files");
                try (ZipInputStream zin = new ZipInputStream(new BufferedInputStream(store.get(zipKey), 1 << 20))) {
                    ZipEntry e;
                    while ((e = zin.getNextEntry()) != null) {
                        Path out = storageDir.resolve(e.getName()).normalize();
                        if (!out.startsWith(storageDir)) throw new ChronoDimException("zip entry escapes target: " + e.getName());
                        Files.createDirectories(out.getParent());
                        byte[] content = zin.readAllBytes();
                        String expect = String.valueOf(checksums.get(e.getName()));
                        String actual = String.format("%016x", XxHash64.hash(content));
                        if (!actual.equals(expect)) {
                            throw new ChronoDimException("restore checksum mismatch for " + e.getName()
                                    + " (expected " + expect + ", got " + actual + ")");
                        }
                        Files.write(out, content);
                        verified++;
                    }
                }
            }

            // Reassemble shipped WAL: whole segments + trailing chunks in offset order.
            Path walDir = targetDir.resolve("wal");
            Files.createDirectories(walDir);
            Map<String, java.util.TreeMap<Long, String>> chunks = new LinkedHashMap<>();
            for (String key : store.list("wal/")) {
                String name = key.substring("wal/".length());
                if (name.contains(".chunk-")) {
                    String seg = name.substring(0, name.indexOf(".chunk-"));
                    long off = Long.parseLong(name.substring(name.indexOf(".chunk-") + 7));
                    chunks.computeIfAbsent(seg, s -> new java.util.TreeMap<>()).put(off, key);
                } else {
                    try (InputStream in = store.get(key); OutputStream out = Files.newOutputStream(walDir.resolve(name))) {
                        in.transferTo(out);
                    }
                }
            }
            for (Map.Entry<String, java.util.TreeMap<Long, String>> e : chunks.entrySet()) {
                Path seg = walDir.resolve(e.getKey());
                long have = Files.exists(seg) ? Files.size(seg) : 0;
                try (OutputStream out = Files.newOutputStream(seg, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND)) {
                    for (Map.Entry<Long, String> c : e.getValue().entrySet()) {
                        if (c.getKey() + store.size(c.getValue()) <= have) continue; // already covered
                        if (c.getKey() > have) {
                            throw new ChronoDimException("WAL chunk gap in " + e.getKey() + " at offset " + c.getKey());
                        }
                        try (InputStream in = store.get(c.getValue())) {
                            byte[] bytes = in.readAllBytes();
                            int skip = (int) (have - c.getKey());
                            out.write(bytes, skip, bytes.length - skip);
                            have += bytes.length - skip;
                        }
                    }
                }
            }

            if (asOfTxn != null) {
                WalTrimmer.trimBeyond(walDir, asOfTxn);
            }

            Map<String, Object> report = new LinkedHashMap<>();
            report.put("snapshot_txn", chosenTxn);
            report.put("snapshot_files_verified", verified);
            report.put("wal_segments", WalTrimmer.count(walDir));
            report.put("as_of_txn", asOfTxn);
            report.put("target", targetDir.toString());
            return report;
        } catch (IOException e) {
            throw new UncheckedIOException("restore failed", e);
        }
    }
}
