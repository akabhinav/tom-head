package io.chronodim.testkit;

import io.chronodim.api.CorruptionException;
import io.chronodim.core.wal.Wal;
import io.chronodim.core.wal.WalReader;
import io.chronodim.core.wal.WalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * OSS-grade WAL torture, modeled on SQLite's exhaustive crash-point testing
 * and Kafka's log corruption injection:
 *
 * <ul>
 *   <li>power loss at EVERY byte offset — recovery must always yield a clean
 *       contiguous prefix of committed transactions, never garbage;</li>
 *   <li>a flipped bit at ANY byte position must never be silent: either the
 *       scan fails loudly (interior) or tail repair drops a suffix — a
 *       surviving record's payload must always be byte-perfect;</li>
 *   <li>the same holds across segment rotation boundaries;</li>
 *   <li>empty and megabyte payloads round-trip; scans are deterministic.</li>
 * </ul>
 */
class WalTortureTest {

    @TempDir
    Path root;

    private static byte[] payload(long i) {
        return ("torture-payload-" + i).getBytes(StandardCharsets.UTF_8);
    }

    private Path freshLog(Path dir, int txns, long segBytes) {
        try (WalWriter w = new WalWriter(dir, segBytes, true, 0, 512, 1)) {
            for (int i = 1; i <= txns; i++) {
                WalWriter.Appended a = w.append(Wal.TXN_COMMIT, i, payload(i));
                w.awaitDurable(a.seq());
            }
        }
        return dir;
    }

    /** Scan with repair; returns recovered txn ids after asserting payload integrity. */
    private static List<Long> recover(Path dir) {
        List<Long> txns = new ArrayList<>();
        WalReader.scan(dir, true, rec -> {
            assertArrayEquals(payload(rec.txnId()), rec.payload(),
                    "recovered record must be byte-perfect, txn " + rec.txnId());
            txns.add(rec.txnId());
        });
        return txns;
    }

    private static void assertCleanPrefix(List<Long> txns, int maxTxns, String ctx) {
        assertTrue(txns.size() <= maxTxns, ctx + ": recovered more than written");
        for (int i = 0; i < txns.size(); i++) {
            assertEquals(i + 1, txns.get(i), ctx + ": recovery must be a contiguous prefix, got " + txns);
        }
    }

    @Test
    void powerLossAtEveryByteOffset() throws IOException {
        Path master = freshLog(root.resolve("master"), 12, 1 << 20);
        Path seg = WalReader.segments(master).get(0);
        byte[] full = Files.readAllBytes(seg);

        int lastPrefix = -1;
        for (int cut = 0; cut <= full.length; cut++) {
            Path dir = root.resolve("cut");
            deleteRecursive(dir);
            Files.createDirectories(dir);
            byte[] truncated = new byte[cut];
            System.arraycopy(full, 0, truncated, 0, cut);
            Files.write(dir.resolve(seg.getFileName().toString()), truncated);

            List<Long> txns = recover(dir);
            assertCleanPrefix(txns, 12, "cut@" + cut);
            assertTrue(txns.size() >= lastPrefix,
                    "cut@" + cut + ": recoverable prefix must be monotone in bytes kept");
            lastPrefix = txns.size();
            // After repair, a strict scan must succeed (the log is clean again).
            WalReader.scan(dir, false, rec -> {});
        }
        assertEquals(12, lastPrefix, "full-length copy must recover everything");
    }

    @Test
    void bitFlipAnywhereIsNeverSilent() throws IOException {
        Path master = freshLog(root.resolve("masterf"), 12, 1 << 20);
        Path seg = WalReader.segments(master).get(0);
        byte[] full = Files.readAllBytes(seg);

        int loud = 0, tailDrops = 0;
        for (int pos = 0; pos < full.length; pos++) {
            Path dir = root.resolve("flip");
            deleteRecursive(dir);
            Files.createDirectories(dir);
            byte[] mutated = full.clone();
            mutated[pos] ^= (byte) 0xFF;
            Files.write(dir.resolve(seg.getFileName().toString()), mutated);

            try {
                List<Long> txns = recover(dir);
                // Silent full recovery of ALL 12 with a flipped byte would mean
                // the CRC missed the corruption — the one forbidden outcome.
                assertTrue(txns.size() < 12, "flip@" + pos + " was silently ignored");
                assertCleanPrefix(txns, 12, "flip@" + pos);
                tailDrops++;
            } catch (CorruptionException expected) {
                loud++;
            }
        }
        assertTrue(loud > 0, "interior flips must fail loudly");
        assertTrue(tailDrops > 0, "tail flips must repair to a clean prefix");
        assertEquals(full.length, loud + tailDrops);
    }

    @Test
    void powerLossAcrossSegmentRotation() throws IOException {
        // Small segments force several rotations.
        Path master = freshLog(root.resolve("masterr"), 60, 2048);
        List<Path> segs = WalReader.segments(master);
        assertTrue(segs.size() > 1, "need rotated segments");
        Path last = segs.get(segs.size() - 1);
        byte[] full = Files.readAllBytes(last);

        Random rnd = new Random(7);
        for (int trial = 0; trial < 200; trial++) {
            int cut = rnd.nextInt(full.length + 1);
            Path dir = root.resolve("rot");
            deleteRecursive(dir);
            Files.createDirectories(dir);
            for (Path s : segs.subList(0, segs.size() - 1)) {
                Files.copy(s, dir.resolve(s.getFileName().toString()));
            }
            byte[] truncated = new byte[cut];
            System.arraycopy(full, 0, truncated, 0, cut);
            Files.write(dir.resolve(last.getFileName().toString()), truncated);

            List<Long> txns = recover(dir);
            assertCleanPrefix(txns, 60, "rot-cut@" + cut);
            // Everything in the closed segments must always survive.
            long inClosed = 0;
            for (Path s : segs.subList(0, segs.size() - 1)) {
                List<Long> one = new ArrayList<>();
                Path solo = root.resolve("solo");
                deleteRecursive(solo);
                Files.createDirectories(solo);
                Files.copy(s, solo.resolve(s.getFileName().toString()));
                WalReader.scan(solo, false, rec -> one.add(rec.txnId()));
                inClosed = Math.max(inClosed, one.isEmpty() ? 0 : one.get(one.size() - 1));
            }
            assertTrue(txns.size() >= inClosed, "rot-cut@" + cut + ": closed-segment txns lost");
        }
    }

    @Test
    void extremePayloadsAndDeterministicScans() {
        Path dir = root.resolve("extreme");
        byte[] empty = new byte[0];
        byte[] huge = new byte[1 << 20];
        new Random(3).nextBytes(huge);
        try (WalWriter w = new WalWriter(dir, 8 << 20, true, 0, 512, 1)) {
            w.awaitDurable(w.append(Wal.TXN_COMMIT, 1, empty).seq());
            w.awaitDurable(w.append(Wal.TXN_COMMIT, 2, huge).seq());
            w.awaitDurable(w.append(Wal.TXN_COMMIT, 3, payload(3)).seq());
        }
        for (int scanNo = 0; scanNo < 2; scanNo++) { // determinism: identical twice
            List<byte[]> got = new ArrayList<>();
            long max = WalReader.scan(dir, false, rec -> got.add(rec.payload()));
            assertEquals(3, max);
            assertArrayEquals(empty, got.get(0));
            assertArrayEquals(huge, got.get(1));
            assertArrayEquals(payload(3), got.get(2));
        }
    }

    private static void deleteRecursive(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (var s = Files.walk(p)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.delete(f);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
    }
}
