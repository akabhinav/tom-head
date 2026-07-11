package io.chronodim.testkit;

import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.KvSnapshot;
import io.chronodim.storage.StorageEngine;
import io.chronodim.storage.lsm.LsmStorageEngine;
import io.chronodim.storage.rocks.RocksDbStorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contract tests both StorageEngine backends must pass (G7 swappability). */
abstract class StorageContractTest {

    @TempDir
    Path dir;

    abstract StorageEngine create(Path dir);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putGetDeleteScan() {
        try (StorageEngine s = create(dir)) {
            AtomicBatch batch = new AtomicBatch()
                    .put(b("a/1"), b("v1"))
                    .put(b("a/2"), b("v2"))
                    .put(b("b/1"), b("v3"));
            s.write(batch, 1);
            assertArrayEquals(b("v1"), s.get(b("a/1")));
            assertNull(s.get(b("a/9")));

            List<byte[]> got = s.multiGet(List.of(b("a/2"), b("nope"), b("b/1")));
            assertArrayEquals(b("v2"), got.get(0));
            assertNull(got.get(1));
            assertArrayEquals(b("v3"), got.get(2));

            s.write(new AtomicBatch().delete(b("a/1")).put(b("a/3"), b("v4")), 2);
            assertNull(s.get(b("a/1")));

            List<String> keys = new ArrayList<>();
            try (CloseableKvIterator it = s.prefixScan(b("a/"))) {
                while (it.hasNext()) keys.add(new String(it.next().key(), StandardCharsets.UTF_8));
            }
            assertEquals(List.of("a/2", "a/3"), keys);
        }
    }

    @Test
    void snapshotIsolation() {
        try (StorageEngine s = create(dir)) {
            s.write(new AtomicBatch().put(b("k1"), b("old")), 1);
            try (KvSnapshot snap = s.snapshot()) {
                s.write(new AtomicBatch().put(b("k1"), b("new")).put(b("k2"), b("x")), 2);
                assertArrayEquals(b("old"), snap.get(b("k1")));
                assertNull(snap.get(b("k2")));
                try (CloseableKvIterator it = snap.prefixScan(new byte[0])) {
                    assertTrue(it.hasNext());
                    assertArrayEquals(b("old"), it.next().value());
                    assertTrue(!it.hasNext());
                }
            }
            assertArrayEquals(b("new"), s.get(b("k1")));
        }
    }

    @Test
    void durabilityWatermarkAndReopen() {
        try (StorageEngine s = create(dir)) {
            s.write(new AtomicBatch().put(b("k"), b("v")), 7);
            s.flush();
            assertTrue(s.durableTxn() >= 7, "flush must advance the durable watermark");
        }
        try (StorageEngine s = create(dir)) {
            assertTrue(s.durableTxn() >= 7);
            assertArrayEquals(b("v"), s.get(b("k")));
        }
    }

    @Test
    void ingestSortedThenReads() {
        try (StorageEngine s = create(dir)) {
            List<KV> sorted = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                sorted.add(new KV(b(String.format("ing/%06d", i)), b("val" + i)));
            }
            s.ingestSorted(sorted.iterator(), 3, sorted.size());
            assertTrue(s.durableTxn() >= 3);
            assertArrayEquals(b("val4321"), s.get(b("ing/004321")));
            int n = 0;
            try (CloseableKvIterator it = s.prefixScan(b("ing/"))) {
                while (it.hasNext()) {
                    it.next();
                    n++;
                }
            }
            assertEquals(5000, n);
            // Later normal writes shadow ingested data.
            s.write(new AtomicBatch().put(b("ing/000000"), b("overridden")), 4);
            assertArrayEquals(b("overridden"), s.get(b("ing/000000")));
        }
        try (StorageEngine s = create(dir)) {
            assertArrayEquals(b("val100"), s.get(b("ing/000100")));
        }
    }

    @Test
    void checkpointIsSelfContained(@TempDir Path cpParent) {
        Path cp = cpParent.resolve("cp");
        try (StorageEngine s = create(dir)) {
            for (int i = 0; i < 100; i++) {
                s.write(new AtomicBatch().put(b("k" + i), b("v" + i)), i);
            }
            s.checkpoint(cp);
        }
        try (StorageEngine s = create(cp)) {
            assertArrayEquals(b("v42"), s.get(b("k42")));
            assertTrue(s.durableTxn() >= 99);
        }
    }

    @Test
    void manyKeysSurviveFlushCycles() {
        try (StorageEngine s = create(dir)) {
            for (int round = 0; round < 5; round++) {
                AtomicBatch batch = new AtomicBatch();
                for (int i = 0; i < 2000; i++) {
                    batch.put(b(String.format("r%02d/k%05d", round, i)), b("v" + round + "-" + i));
                }
                s.write(batch, round);
                s.flush();
            }
            assertArrayEquals(b("v3-777"), s.get(b("r03/k00777")));
            int n = 0;
            try (CloseableKvIterator it = s.prefixScan(new byte[0])) {
                byte[] prev = null;
                while (it.hasNext()) {
                    KV kv = it.next();
                    if (prev != null) {
                        assertTrue(io.chronodim.storage.util.Bytes.compare(prev, kv.key()) < 0, "scan must be sorted");
                    }
                    prev = kv.key();
                    n++;
                }
            }
            assertTrue(n >= 10_000);
        }
    }

    @Test
    void emptyIteratorContractsHold() {
        try (StorageEngine s = create(dir)) {
            try (CloseableKvIterator it = s.prefixScan(b("nothing/"))) {
                assertTrue(!it.hasNext());
                boolean threw = false;
                try {
                    it.next();
                } catch (NoSuchElementException e) {
                    threw = true;
                }
                assertTrue(threw);
            }
            Iterator<KV> empty = List.<KV>of().iterator();
            s.ingestSorted(empty, 1, 0);
        }
    }
}

class LsmStorageTest extends StorageContractTest {
    @Override
    StorageEngine create(Path dir) {
        // Tiny memtable + aggressive compaction to exercise flush/merge/tombstone paths.
        return new LsmStorageEngine(dir, 64 << 10, 3, true);
    }
}

class RocksDbStorageTest extends StorageContractTest {
    @Override
    StorageEngine create(Path dir) {
        return new RocksDbStorageEngine(dir, 8 << 20, 32 << 20);
    }
}
