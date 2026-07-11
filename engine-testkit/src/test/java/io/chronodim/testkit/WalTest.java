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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalTest {

    @TempDir
    Path dir;

    private static byte[] payload(int i) {
        return ("payload-" + i).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendScanRoundTripAcrossSegments() {
        try (WalWriter w = new WalWriter(dir, 4096, true, 0, 512, 1)) {
            for (int i = 1; i <= 200; i++) {
                WalWriter.Appended a = w.append(Wal.TXN_COMMIT, i, payload(i));
                w.awaitDurable(a.seq());
            }
        }
        assertTrue(WalReader.segments(dir).size() > 1, "should have rolled segments");
        List<Long> txns = new ArrayList<>();
        long max = WalReader.scan(dir, false, rec -> {
            txns.add(rec.txnId());
            assertEquals("payload-" + rec.txnId(), new String(rec.payload(), StandardCharsets.UTF_8));
        });
        assertEquals(200, max);
        assertEquals(200, txns.size());
        for (int i = 1; i <= 200; i++) assertEquals(i, txns.get(i - 1));
    }

    @Test
    void tornTailIsTruncatedOnRecovery() throws IOException {
        try (WalWriter w = new WalWriter(dir, 1 << 20, true, 0, 512, 1)) {
            for (int i = 1; i <= 10; i++) w.append(Wal.TXN_COMMIT, i, payload(i));
        }
        Path seg = WalReader.segments(dir).get(0);
        // Simulate a torn write: garbage frame appended at the tail.
        Files.write(seg, new byte[]{1, 2, 3, 4, 5}, StandardOpenOption.APPEND);
        List<Long> txns = new ArrayList<>();
        WalReader.scan(dir, true, rec -> txns.add(rec.txnId()));
        assertEquals(10, txns.size());
        // After repair a clean scan succeeds.
        WalReader.scan(dir, false, rec -> {});
    }

    @Test
    void interiorCorruptionFailsLoudly() throws IOException {
        try (WalWriter w = new WalWriter(dir, 1 << 20, true, 0, 512, 1)) {
            for (int i = 1; i <= 10; i++) w.append(Wal.TXN_COMMIT, i, payload(i));
        }
        Path seg = WalReader.segments(dir).get(0);
        byte[] content = Files.readAllBytes(seg);
        content[20] ^= (byte) 0xFF; // flip a byte inside the first record
        Files.write(seg, content);
        assertThrows(CorruptionException.class, () -> WalReader.scan(dir, true, rec -> {}));
    }

    @Test
    void groupCommitUnderConcurrency() throws Exception {
        try (WalWriter w = new WalWriter(dir, 8 << 20, true, 2000, 512, 1)) {
            int threads = 8, perThread = 50;
            Thread[] ts = new Thread[threads];
            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                ts[t] = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        WalWriter.Appended a = w.append(Wal.TXN_COMMIT, base + i + 1, payload(base + i));
                        w.awaitDurable(a.seq());
                    }
                });
                ts[t].start();
            }
            for (Thread t : ts) t.join();
            long fsyncs = (Long) w.stats().get("fsyncs");
            assertTrue(fsyncs <= threads * perThread, "fsyncs=" + fsyncs);
        }
        List<Long> seen = new ArrayList<>();
        WalReader.scan(dir, false, rec -> seen.add(rec.txnId()));
        assertEquals(400, seen.size());
    }
}
