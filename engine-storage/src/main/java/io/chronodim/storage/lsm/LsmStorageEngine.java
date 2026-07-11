package io.chronodim.storage.lsm;

import io.chronodim.api.ChronoDimException;
import io.chronodim.api.CorruptionException;
import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.KvSnapshot;
import io.chronodim.storage.StorageEngine;
import io.chronodim.storage.util.Bytes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * Pure-Java LSM implementation of {@link StorageEngine}.
 *
 * <p>Design: single active memtable; snapshot/flush rotate it into a
 * newest-first list of frozen memtables; a single maintenance thread flushes
 * frozen memtables into sorted segments and compacts segments size-tiered.
 * Durability is delegated to the engine WAL above: the storage manifest records
 * the highest txn fully contained in segments ({@link #durableTxn()}), and the
 * engine replays its WAL after that watermark on restart.
 */
public final class LsmStorageEngine implements StorageEngine {

    private static final String MANIFEST = "MANIFEST";
    private static final String MANIFEST_MAGIC = "CDMF1";
    private static final String SEG_SUFFIX = ".cds";

    /** Immutable view of the store; readers volatile-read it, mutators swap under {@code this}. */
    private record State(Memtable active, List<Memtable> frozenNewestFirst, List<Segment> segmentsNewestFirst) {}

    private final Path dir;
    private final long flushBytes;
    private final int maxSegments;
    private final boolean fsync;
    private final ExecutorService maintenance =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "chronodim-lsm-maintenance");
                t.setDaemon(true);
                return t;
            });

    private volatile State state;
    private volatile long durableTxnWatermark = -1;
    private long nextSegId;
    private volatile boolean flushScheduled;
    private volatile boolean closed;

    public LsmStorageEngine(Path dir, long flushBytes, int maxSegments, boolean fsync) {
        this.dir = dir;
        this.flushBytes = flushBytes;
        this.maxSegments = maxSegments;
        this.fsync = fsync;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<Segment> segments = loadManifestAndSegments();
        this.state = new State(new Memtable(), List.of(), segments);
    }

    // ---- reads --------------------------------------------------------------

    @Override
    public byte[] get(byte[] key) {
        List<Segment> retained;
        State s;
        synchronized (this) {
            s = state;
            retained = s.segmentsNewestFirst;
            for (Segment seg : retained) seg.retain();
        }
        try {
            return lookup(s, key);
        } finally {
            for (Segment seg : retained) seg.release();
        }
    }

    @Override
    public List<byte[]> multiGet(List<byte[]> keys) {
        List<Segment> retained;
        State s;
        synchronized (this) {
            s = state;
            retained = s.segmentsNewestFirst;
            for (Segment seg : retained) seg.retain();
        }
        try {
            List<byte[]> out = new ArrayList<>(keys.size());
            for (byte[] k : keys) out.add(lookup(s, k));
            return out;
        } finally {
            for (Segment seg : retained) seg.release();
        }
    }

    private static byte[] lookup(State s, byte[] key) {
        byte[] v = s.active.getRaw(key);
        if (v != null) return v == Memtable.TOMBSTONE ? null : v;
        for (Memtable m : s.frozenNewestFirst) {
            v = m.getRaw(key);
            if (v != null) return v == Memtable.TOMBSTONE ? null : v;
        }
        for (Segment seg : s.segmentsNewestFirst) {
            v = seg.get(key);
            if (v != null) return v == Memtable.TOMBSTONE ? null : v;
        }
        return null;
    }

    @Override
    public CloseableKvIterator prefixScan(byte[] prefix) {
        KvSnapshot snap = snapshot();
        CloseableKvIterator inner = snap.prefixScan(prefix);
        return new CloseableKvIterator() {
            @Override public boolean hasNext() { return inner.hasNext(); }
            @Override public KV next() { return inner.next(); }
            @Override public void close() { inner.close(); snap.close(); }
        };
    }

    @Override
    public KvSnapshot snapshot() {
        synchronized (this) {
            rotateLocked();
            State s = state;
            for (Segment seg : s.segmentsNewestFirst) seg.retain();
            return new LsmSnapshot(s.frozenNewestFirst, s.segmentsNewestFirst);
        }
    }

    // ---- writes -------------------------------------------------------------

    @Override
    public void write(AtomicBatch batch, long txnId) {
        boolean needFlush = false;
        synchronized (this) {
            checkOpen();
            Memtable active = state.active;
            for (AtomicBatch.Mutation m : batch.mutations()) {
                switch (m) {
                    case AtomicBatch.Put p -> active.put(p.key(), p.value(), txnId);
                    case AtomicBatch.Delete d -> active.put(d.key(), Memtable.TOMBSTONE, txnId);
                }
            }
            if (active.approximateBytes() >= flushBytes) {
                rotateLocked();
                needFlush = true;
            }
        }
        if (needFlush) scheduleFlush();
    }

    @Override
    public void ingestSorted(Iterator<KV> sortedKvs, long txnId, long expectedKeys) {
        checkOpen();
        flush(); // everything older must already be in segments so ordering stays correct
        submitAndWait(() -> {
            long id;
            synchronized (this) {
                id = nextSegId++;
            }
            Path p = segPath(id);
            SegmentWriter w = new SegmentWriter(p, Math.max(expectedKeys, 1024), fsync);
            long n = 0;
            try (w) {
                while (sortedKvs.hasNext()) {
                    KV kv = sortedKvs.next();
                    w.append(kv.key(), kv.value(), false);
                    n++;
                }
                if (n > 0) w.finish();
            }
            if (n == 0) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                synchronized (this) {
                    if (txnId > durableTxnWatermark) durableTxnWatermark = txnId;
                }
                writeManifest();
                return null;
            }
            Segment seg = Segment.open(p, id);
            synchronized (this) {
                List<Segment> segs = new ArrayList<>();
                segs.add(seg);
                segs.addAll(state.segmentsNewestFirst);
                state = new State(state.active, state.frozenNewestFirst, List.copyOf(segs));
                if (txnId > durableTxnWatermark) durableTxnWatermark = txnId;
            }
            writeManifest();
            maybeCompact();
            return null;
        });
    }

    @Override
    public void flush() {
        checkOpen();
        submitAndWait(() -> {
            doFlush();
            return null;
        });
    }

    @Override
    public void checkpoint(Path targetDir) {
        checkOpen();
        flush();
        submitAndWait(() -> {
            Files.createDirectories(targetDir);
            List<Segment> segs;
            synchronized (this) {
                segs = state.segmentsNewestFirst;
                for (Segment s : segs) s.retain();
            }
            try {
                for (Segment s : segs) {
                    Files.copy(s.path, targetDir.resolve(s.path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
                }
                Files.copy(dir.resolve(MANIFEST), targetDir.resolve(MANIFEST), StandardCopyOption.REPLACE_EXISTING);
                if (fsync) fsyncDir(targetDir);
            } finally {
                for (Segment s : segs) s.release();
            }
            return null;
        });
    }

    @Override
    public long durableTxn() {
        return durableTxnWatermark;
    }

    @Override
    public Map<String, Object> stats() {
        State s = state;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("memtable_bytes", s.active.approximateBytes());
        m.put("memtable_entries", s.active.count());
        m.put("frozen_memtables", s.frozenNewestFirst.size());
        m.put("segments", s.segmentsNewestFirst.size());
        long entries = 0;
        for (Segment seg : s.segmentsNewestFirst) entries += seg.entryCount();
        m.put("segment_entries", entries);
        m.put("durable_txn", durableTxnWatermark);
        return m;
    }

    @Override
    public void close() {
        if (closed) return;
        try {
            flush();
        } finally {
            closed = true;
            maintenance.shutdown();
            try {
                if (!maintenance.awaitTermination(60, TimeUnit.SECONDS)) {
                    maintenance.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            synchronized (this) {
                for (Segment s : state.segmentsNewestFirst) s.release();
                state = new State(new Memtable(), List.of(), List.of());
            }
        }
    }

    // ---- maintenance ----------------------------------------------------------

    /** Must hold {@code this}. */
    private void rotateLocked() {
        Memtable active = state.active;
        if (active.isEmpty()) return;
        List<Memtable> frozen = new ArrayList<>();
        frozen.add(active);
        frozen.addAll(state.frozenNewestFirst);
        state = new State(new Memtable(), List.copyOf(frozen), state.segmentsNewestFirst);
    }

    private void scheduleFlush() {
        if (flushScheduled || closed) return;
        flushScheduled = true;
        maintenance.submit(() -> {
            flushScheduled = false;
            try {
                doFlush();
            } catch (Throwable t) {
                // A failed flush leaves data in memtables; the WAL still owns durability.
                // Surface loudly rather than silently degrading.
                t.printStackTrace();
            }
        });
    }

    /** Runs on the maintenance thread only. */
    private void doFlush() throws IOException {
        List<Memtable> toFlush;
        synchronized (this) {
            rotateLocked();
            toFlush = state.frozenNewestFirst;
        }
        if (toFlush.isEmpty()) {
            writeManifest();
            return;
        }
        long expected = 0;
        long maxTxn = -1;
        List<Iterator<LsmEntry>> sources = new ArrayList<>(toFlush.size());
        for (Memtable m : toFlush) {
            expected += m.count();
            maxTxn = Math.max(maxTxn, m.maxTxn());
            sources.add(wrap(m.all()));
        }
        long id;
        synchronized (this) {
            id = nextSegId++;
        }
        Path p = segPath(id);
        long n = 0;
        try (SegmentWriter w = new SegmentWriter(p, expected, fsync);
             MergeIterator merge = new MergeIterator(sources)) {
            while (merge.hasNext()) {
                LsmEntry e = merge.next();
                w.append(e.key(), e.value(), e.tombstone());
                n++;
            }
            if (n > 0) w.finish();
        }
        Segment seg = n > 0 ? Segment.open(p, id) : null;
        if (n == 0) Files.deleteIfExists(p);
        synchronized (this) {
            List<Memtable> remaining = new ArrayList<>(state.frozenNewestFirst);
            remaining.removeAll(identitySet(toFlush, remaining));
            List<Segment> segs = new ArrayList<>();
            if (seg != null) segs.add(seg);
            segs.addAll(state.segmentsNewestFirst);
            state = new State(state.active, List.copyOf(remaining), List.copyOf(segs));
            if (maxTxn > durableTxnWatermark) durableTxnWatermark = maxTxn;
        }
        writeManifest();
        maybeCompact();
    }

    /** Runs on the maintenance thread only. */
    private void maybeCompact() throws IOException {
        List<Segment> segs;
        synchronized (this) {
            segs = state.segmentsNewestFirst;
        }
        if (segs.size() <= maxSegments) return;

        long expected = 0;
        List<Iterator<LsmEntry>> sources = new ArrayList<>(segs.size());
        for (Segment s : segs) {
            expected += s.entryCount();
            sources.add(s.iterate(Bytes.EMPTY));
        }
        long id;
        synchronized (this) {
            id = nextSegId++;
        }
        Path p = segPath(id);
        long n = 0;
        try (SegmentWriter w = new SegmentWriter(p, expected, fsync);
             MergeIterator merge = new MergeIterator(sources)) {
            while (merge.hasNext()) {
                LsmEntry e = merge.next();
                if (e.tombstone()) continue; // full compaction: nothing older can resurface
                w.append(e.key(), e.value(), false);
                n++;
            }
            if (n > 0) w.finish();
        }
        Segment merged = n > 0 ? Segment.open(p, id) : null;
        if (n == 0) Files.deleteIfExists(p);
        synchronized (this) {
            // Only this thread mutates segments, so state.segments == segs still.
            List<Segment> now = new ArrayList<>(state.segmentsNewestFirst);
            if (!now.equals(segs)) throw new IllegalStateException("concurrent segment mutation during compaction");
            List<Segment> replaced = merged == null ? List.of() : List.of(merged);
            state = new State(state.active, state.frozenNewestFirst, replaced);
        }
        writeManifest();
        for (Segment s : segs) {
            s.deleteWhenUnreferenced();
            s.release();
        }
    }

    private static List<Memtable> identitySet(List<Memtable> toFlush, List<Memtable> in) {
        List<Memtable> out = new ArrayList<>();
        for (Memtable m : in) {
            for (Memtable f : toFlush) {
                if (m == f) {
                    out.add(m);
                    break;
                }
            }
        }
        return out;
    }

    private static Iterator<LsmEntry> wrap(Iterator<Map.Entry<byte[], byte[]>> it) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public LsmEntry next() {
                Map.Entry<byte[], byte[]> e = it.next();
                return new LsmEntry(e.getKey(), e.getValue());
            }
        };
    }

    private <T> T submitAndWait(java.util.concurrent.Callable<T> task) {
        try {
            Future<T> f = maintenance.submit(task);
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ChronoDimException("interrupted waiting for storage maintenance", e);
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException re) throw re;
            throw new ChronoDimException("storage maintenance failed", c);
        }
    }

    private void checkOpen() {
        if (closed) throw new ChronoDimException("storage engine is closed");
    }

    private Path segPath(long id) {
        return dir.resolve("seg-" + String.format("%012d", id) + SEG_SUFFIX);
    }

    // ---- manifest --------------------------------------------------------------

    private void writeManifest() {
        StringBuilder sb = new StringBuilder();
        synchronized (this) {
            sb.append(MANIFEST_MAGIC).append('\n');
            sb.append("nextSegId=").append(nextSegId).append('\n');
            sb.append("durableTxn=").append(durableTxnWatermark).append('\n');
            for (Segment s : state.segmentsNewestFirst) {
                sb.append("segment=").append(s.path.getFileName()).append('\n');
            }
        }
        CRC32C crc = new CRC32C();
        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        crc.update(body);
        sb.append("crc=").append(Long.toHexString(crc.getValue())).append('\n');
        try {
            Path tmp = dir.resolve(MANIFEST + ".tmp");
            Files.writeString(tmp, sb.toString());
            if (fsync) {
                try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
            }
            Files.move(tmp, dir.resolve(MANIFEST), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            if (fsync) fsyncDir(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write storage manifest", e);
        }
    }

    private List<Segment> loadManifestAndSegments() {
        Path mf = dir.resolve(MANIFEST);
        List<Segment> segments = new ArrayList<>();
        java.util.Set<String> live = new java.util.HashSet<>();
        if (Files.exists(mf)) {
            try {
                List<String> lines = Files.readAllLines(mf, StandardCharsets.UTF_8);
                if (lines.isEmpty() || !lines.get(0).equals(MANIFEST_MAGIC)) {
                    throw new CorruptionException("bad storage manifest magic in " + mf);
                }
                String crcLine = lines.get(lines.size() - 1);
                if (!crcLine.startsWith("crc=")) throw new CorruptionException("storage manifest missing crc: " + mf);
                String body = String.join("\n", lines.subList(0, lines.size() - 1)) + "\n";
                CRC32C crc = new CRC32C();
                crc.update(body.getBytes(StandardCharsets.UTF_8));
                if (crc.getValue() != Long.parseUnsignedLong(crcLine.substring(4), 16)) {
                    throw new CorruptionException("storage manifest crc mismatch: " + mf);
                }
                for (String line : lines.subList(1, lines.size() - 1)) {
                    if (line.startsWith("nextSegId=")) {
                        nextSegId = Long.parseLong(line.substring(10));
                    } else if (line.startsWith("durableTxn=")) {
                        durableTxnWatermark = Long.parseLong(line.substring(11));
                    } else if (line.startsWith("segment=")) {
                        String name = line.substring(8);
                        live.add(name);
                        long id = Long.parseLong(name.substring(4, name.length() - SEG_SUFFIX.length()));
                        segments.add(Segment.open(dir.resolve(name), id));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("cannot read storage manifest", e);
            }
        }
        // Remove orphans from crashes between segment write and manifest update.
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "seg-*" + SEG_SUFFIX)) {
            for (Path p : ds) {
                if (!live.contains(p.getFileName().toString())) {
                    Files.deleteIfExists(p);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return List.copyOf(segments);
    }

    private static void fsyncDir(Path d) {
        try (FileChannel ch = FileChannel.open(d, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            // Directory fsync is not supported on all platforms; the manifest itself is CRC'd.
        }
    }

    // ---- snapshot ----------------------------------------------------------------

    private static final class LsmSnapshot implements KvSnapshot {
        private final List<Memtable> frozen;
        private final List<Segment> segments;
        private volatile boolean closed;

        LsmSnapshot(List<Memtable> frozen, List<Segment> segments) {
            this.frozen = frozen;
            this.segments = segments;
        }

        @Override
        public byte[] get(byte[] key) {
            for (Memtable m : frozen) {
                byte[] v = m.getRaw(key);
                if (v != null) return v == Memtable.TOMBSTONE ? null : v;
            }
            for (Segment s : segments) {
                byte[] v = s.get(key);
                if (v != null) return v == Memtable.TOMBSTONE ? null : v;
            }
            return null;
        }

        @Override
        public CloseableKvIterator prefixScan(byte[] prefix) {
            List<Iterator<LsmEntry>> sources = new ArrayList<>(frozen.size() + segments.size());
            for (Memtable m : frozen) sources.add(wrap(m.range(prefix)));
            for (Segment s : segments) sources.add(s.iterate(prefix));
            MergeIterator merge = new MergeIterator(sources);
            return new CloseableKvIterator() {
                private KV next;
                { advance(); }

                private void advance() {
                    next = null;
                    while (merge.hasNext()) {
                        LsmEntry e = merge.next();
                        if (e.tombstone()) continue;
                        if (!Bytes.hasPrefix(e.key(), prefix)) continue;
                        next = new KV(e.key(), e.value());
                        return;
                    }
                }

                @Override public boolean hasNext() { return next != null; }

                @Override public KV next() {
                    if (next == null) throw new NoSuchElementException();
                    KV kv = next;
                    advance();
                    return kv;
                }

                @Override public void close() { merge.close(); }
            };
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            for (Segment s : segments) s.release();
        }
    }
}
