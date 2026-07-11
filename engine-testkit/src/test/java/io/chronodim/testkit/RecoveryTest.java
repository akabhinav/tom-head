package io.chronodim.testkit;

import io.chronodim.api.ApplyBatch;
import io.chronodim.api.AuditManifest;
import io.chronodim.api.ChronoDim;
import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.InputRow;
import io.chronodim.api.TableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash-consistency (R-WAL-5 flavor runnable in-process): the WAL is authoritative;
 * hot-store state may lag arbitrarily — even be destroyed entirely — and recovery
 * must reproduce exactly the acknowledged state.
 */
class RecoveryTest {

    @TempDir
    Path root;

    private static EngineOptions opts(String backend) {
        return EngineOptions.builder()
                .storageBackend(EngineOptions.StorageBackend.valueOf(backend))
                .groupCommitWindowMicros(0)
                .build();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ROCKSDB", "LSM"})
    void hotStoreDestroyedThenRebuiltFromWal(String backend) throws IOException {
        Path dir = root.resolve("db-" + backend);
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(42, DataGen.StreamOptions.defaults());

        try (Engine e = ChronoDim.open(dir, opts(backend))) {
            e.createTable(cfg);
            for (int b = 0; b < 10; b++) {
                List<InputRow> rows = gen.nextBatch(30);
                AuditManifest m = e.apply(ApplyBatch.single("b" + b, "t", rows));
                ref.applyBatch(rows, m.startedAtMicros());
            }
        }

        // Disaster: the entire hot store is gone. Only the WAL survives.
        deleteRecursive(dir.resolve("storage"));

        try (Engine e = ChronoDim.open(dir, opts(backend))) {
            DifferentialTest.compareFullState(e, ref, cfg);
            // Idempotency map survived too.
            assertTrue(e.apply(ApplyBatch.single("b3", "t", List.of())).alreadyApplied());
        }
    }

    @Test
    void crashImageMidRunRecoversExactly() throws IOException {
        // LSM backend: its files are safe to copy at any quiesced point, giving us a
        // faithful "kill -9" disk image (fsynced WAL + possibly-stale hot store).
        Path dir = root.resolve("db-crash");
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(7, DataGen.StreamOptions.defaults());
        Path image = root.resolve("crash-image");

        try (Engine e = ChronoDim.open(dir, opts("LSM"))) {
            e.createTable(cfg);
            for (int b = 0; b < 8; b++) {
                List<InputRow> rows = gen.nextBatch(25);
                AuditManifest m = e.apply(ApplyBatch.single("b" + b, "t", rows));
                ref.applyBatch(rows, m.startedAtMicros());
            }
            // Copy the data dir while the engine is still open — memtable contents are
            // NOT in the copied storage files, only in the WAL.
            copyRecursive(dir, image);
        }

        try (Engine e = ChronoDim.open(image, opts("LSM"))) {
            DifferentialTest.compareFullState(e, ref, cfg);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ROCKSDB", "LSM"})
    void ackedTransactionsSurviveReopenLoop(String backend) {
        Path dir = root.resolve("loop-" + backend);
        TableConfig cfg = DataGen.testTable("t");
        ReferenceScd2 ref = new ReferenceScd2(cfg);
        DataGen gen = new DataGen(11, DataGen.StreamOptions.defaults());

        for (int session = 0; session < 4; session++) {
            try (Engine e = ChronoDim.open(dir, opts(backend))) {
                if (session == 0) e.createTable(cfg);
                for (int b = 0; b < 3; b++) {
                    List<InputRow> rows = gen.nextBatch(20);
                    AuditManifest m = e.apply(ApplyBatch.single("s" + session + "-b" + b, "t", rows));
                    ref.applyBatch(rows, m.startedAtMicros());
                }
                DifferentialTest.compareFullState(e, ref, cfg);
            }
        }

        // txn ids stay monotonic across sessions (R-WAL-4)
        try (Engine e = ChronoDim.open(dir, opts(backend))) {
            List<AuditManifest> manifests = e.listManifests(null, 100);
            long prev = Long.MAX_VALUE;
            for (AuditManifest m : manifests) { // newest first
                assertTrue(m.txnId() < prev || prev == Long.MAX_VALUE);
                prev = m.txnId();
            }
            assertEquals(12, manifests.size());
        }
    }

    private static void deleteRecursive(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> walk = Files.walk(p)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.deleteIfExists(f);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
    }

    private static void copyRecursive(Path from, Path to) throws IOException {
        try (Stream<Path> walk = Files.walk(from)) {
            for (Path src : walk.toList()) {
                Path dst = to.resolve(from.relativize(src).toString());
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dst);
                } else if (!src.getFileName().toString().equals("LOCK")) {
                    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
