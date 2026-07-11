package io.chronodim.storage.lsm;

import io.chronodim.storage.util.Bytes;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge over sorted sources. Lower source index = newer data = wins on key
 * ties. Emits each key once; tombstones are emitted (callers filter or drop).
 */
final class MergeIterator implements Iterator<LsmEntry>, AutoCloseable {

    private final PriorityQueue<Cursor> heap;
    private final List<? extends Iterator<LsmEntry>> sources;
    private LsmEntry next;
    private byte[] lastKey;

    MergeIterator(List<? extends Iterator<LsmEntry>> newestFirstSources) {
        this.sources = newestFirstSources;
        this.heap = new PriorityQueue<>((a, b) -> {
            int c = Bytes.compare(a.entry.key(), b.entry.key());
            return c != 0 ? c : Integer.compare(a.priority, b.priority);
        });
        for (int i = 0; i < newestFirstSources.size(); i++) {
            Iterator<LsmEntry> it = newestFirstSources.get(i);
            if (it.hasNext()) heap.add(new Cursor(i, it, it.next()));
        }
        advance();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public LsmEntry next() {
        if (next == null) throw new NoSuchElementException();
        LsmEntry e = next;
        advance();
        return e;
    }

    private void advance() {
        next = null;
        while (!heap.isEmpty()) {
            Cursor c = heap.poll();
            LsmEntry e = c.entry;
            if (c.it.hasNext()) {
                c.entry = c.it.next();
                heap.add(c);
            }
            if (lastKey != null && Bytes.compare(lastKey, e.key()) == 0) {
                continue; // shadowed by a newer source
            }
            lastKey = e.key();
            next = e;
            return;
        }
    }

    @Override
    public void close() {
        for (Iterator<LsmEntry> it : sources) {
            if (it instanceof AutoCloseable ac) {
                try {
                    ac.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static final class Cursor {
        final int priority;
        final Iterator<LsmEntry> it;
        LsmEntry entry;

        Cursor(int priority, Iterator<LsmEntry> it, LsmEntry entry) {
            this.priority = priority;
            this.it = it;
            this.entry = entry;
        }
    }
}
