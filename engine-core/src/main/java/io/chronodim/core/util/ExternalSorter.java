package io.chronodim.core.util;

import io.chronodim.storage.KV;
import io.chronodim.storage.util.Bytes;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Spill-to-disk merge sort of KV pairs by unsigned key order — the bulk-backfill
 * path (R-APPLY-8) sorts arbitrarily large inputs in bounded memory.
 * Duplicate keys are preserved in insertion order (stable).
 */
public final class ExternalSorter implements AutoCloseable {

    private final Path tmpDir;
    private final long spillBytes;
    private final List<Path> runs = new ArrayList<>();
    private List<KV> buffer = new ArrayList<>();
    private long bufferedBytes;
    private long count;

    public ExternalSorter(Path tmpDir, long spillBytes) {
        this.tmpDir = tmpDir;
        this.spillBytes = spillBytes;
        try {
            Files.createDirectories(tmpDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void add(byte[] key, byte[] value) {
        buffer.add(new KV(key, value));
        bufferedBytes += key.length + value.length + 48;
        count++;
        if (bufferedBytes >= spillBytes) spill();
    }

    public long count() {
        return count;
    }

    private void spill() {
        if (buffer.isEmpty()) return;
        sortStable(buffer);
        Path run = tmpDir.resolve("run-" + runs.size() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(run), 1 << 20))) {
            for (KV kv : buffer) {
                out.writeInt(kv.key().length);
                out.writeInt(kv.value().length);
                out.write(kv.key());
                out.write(kv.value());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        runs.add(run);
        buffer = new ArrayList<>();
        bufferedBytes = 0;
    }

    private static void sortStable(List<KV> list) {
        list.sort((a, b) -> Bytes.compare(a.key(), b.key())); // List.sort is stable (TimSort)
    }

    /** Sorted iterator over everything added. Call once; the sorter is consumed. */
    public Iterator<KV> sortedIterator() {
        if (runs.isEmpty()) {
            sortStable(buffer);
            return buffer.iterator();
        }
        spill();
        List<RunReader> readers = new ArrayList<>();
        for (Path r : runs) readers.add(new RunReader(r, readers.size()));
        PriorityQueue<RunReader> heap = new PriorityQueue<>((a, b) -> {
            int c = Bytes.compare(a.current.key(), b.current.key());
            return c != 0 ? c : Integer.compare(a.index, b.index); // stability across runs
        });
        for (RunReader r : readers) {
            if (r.advance()) heap.add(r);
        }
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return !heap.isEmpty();
            }

            @Override
            public KV next() {
                RunReader r = heap.poll();
                if (r == null) throw new NoSuchElementException();
                KV kv = r.current;
                if (r.advance()) heap.add(r);
                else r.close();
                return kv;
            }
        };
    }

    @Override
    public void close() {
        for (Path r : runs) {
            try {
                Files.deleteIfExists(r);
            } catch (IOException ignored) {
                // best-effort temp cleanup
            }
        }
        buffer = new ArrayList<>();
    }

    private static final class RunReader {
        final int index;
        final DataInputStream in;
        final Path path;
        KV current;

        RunReader(Path path, int index) {
            this.path = path;
            this.index = index;
            try {
                this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 20));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        boolean advance() {
            try {
                int klen;
                try {
                    klen = in.readInt();
                } catch (EOFException eof) {
                    current = null;
                    return false;
                }
                int vlen = in.readInt();
                byte[] k = new byte[klen];
                byte[] v = new byte[vlen];
                in.readFully(k);
                in.readFully(v);
                current = new KV(k, v);
                return true;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void close() {
            try {
                in.close();
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // best-effort temp cleanup
            }
        }
    }
}
