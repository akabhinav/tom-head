package io.chronodim.storage.lsm;

import io.chronodim.api.CorruptionException;
import io.chronodim.storage.util.Bytes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

/**
 * Read-side of an on-disk sorted segment. Thread-safe: all reads are positional
 * (pread). Reference-counted so snapshots keep compacted-away files alive until
 * released.
 */
final class Segment implements AutoCloseable {

    final Path path;
    final long id;
    private final FileChannel channel;
    private final BloomFilter bloom;
    private final byte[][] sparseKeys;
    private final long[] sparseOffsets;
    private final long entryCount;
    private final long dataEnd; // == indexOff
    private final AtomicInteger refs = new AtomicInteger(1);
    private volatile boolean deleteOnClose;

    private Segment(Path path, long id, FileChannel ch, BloomFilter bloom,
                    byte[][] sparseKeys, long[] sparseOffsets, long entryCount, long dataEnd) {
        this.path = path;
        this.id = id;
        this.channel = ch;
        this.bloom = bloom;
        this.sparseKeys = sparseKeys;
        this.sparseOffsets = sparseOffsets;
        this.entryCount = entryCount;
        this.dataEnd = dataEnd;
    }

    static Segment open(Path path, long id) {
        try {
            FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
            long size = ch.size();
            if (size < 4 + SegmentWriter.FOOTER_LEN) throw new CorruptionException("segment too small: " + path);
            ByteBuffer footer = ByteBuffer.allocate(SegmentWriter.FOOTER_LEN);
            readFully(ch, footer, size - SegmentWriter.FOOTER_LEN);
            footer.flip();
            long indexOff = footer.getLong();
            long bloomOff = footer.getLong();
            long entryCount = footer.getLong();
            byte version = footer.get();
            int crcStored = footer.getInt();
            byte[] magic = new byte[4];
            footer.get(magic);
            if (!java.util.Arrays.equals(magic, SegmentWriter.MAGIC)) {
                throw new CorruptionException("segment bad magic: " + path);
            }
            CRC32C crc = new CRC32C();
            crc.update(footer.array(), 0, 25);
            if ((int) crc.getValue() != crcStored) throw new CorruptionException("segment footer CRC mismatch: " + path);
            if (version != SegmentWriter.FORMAT_VERSION) {
                throw new CorruptionException("segment format version " + version + " unsupported: " + path);
            }

            // index
            ByteBuffer idx = ByteBuffer.allocate((int) (bloomOff - indexOff));
            readFully(ch, idx, indexOff);
            idx.flip();
            long cnt = idx.getLong();
            idx.getInt(); // sparse interval (informational; format constant)
            int sparseCount = idx.getInt();
            byte[][] sk = new byte[sparseCount][];
            long[] so = new long[sparseCount];
            for (int i = 0; i < sparseCount; i++) {
                byte[] k = new byte[idx.getInt()];
                idx.get(k);
                sk[i] = k;
                so[i] = idx.getLong();
            }
            if (cnt != entryCount) throw new CorruptionException("segment index/footer count mismatch: " + path);

            // bloom
            ByteBuffer bb = ByteBuffer.allocate((int) (size - SegmentWriter.FOOTER_LEN - bloomOff));
            readFully(ch, bb, bloomOff);
            bb.flip();
            int words = bb.getInt();
            int numHashes = bb.getInt();
            long[] bits = new long[words];
            for (int i = 0; i < words; i++) bits[i] = bb.getLong();

            return new Segment(path, id, ch, BloomFilter.from(bits, numHashes), sk, so, entryCount, indexOff);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open segment " + path, e);
        }
    }

    long entryCount() {
        return entryCount;
    }

    void retain() {
        int r = refs.incrementAndGet();
        if (r <= 1) throw new IllegalStateException("segment retained after release: " + path);
    }

    void release() {
        if (refs.decrementAndGet() == 0) {
            try {
                channel.close();
                if (deleteOnClose) Files.deleteIfExists(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    void deleteWhenUnreferenced() {
        deleteOnClose = true;
    }

    @Override
    public void close() {
        release();
    }

    /**
     * Point lookup. Returns the stored value, {@link Memtable#TOMBSTONE} (identity)
     * for a tombstone, or null when the key is absent from this segment.
     */
    byte[] get(byte[] key) {
        if (entryCount == 0 || !bloom.mightContain(key)) return null;
        int block = floorBlock(key);
        if (block < 0) return null;
        long start = sparseOffsets[block];
        long end = block + 1 < sparseOffsets.length ? sparseOffsets[block + 1] : dataEnd;
        byte[] buf = new byte[(int) (end - start)];
        try {
            readFully(channel, ByteBuffer.wrap(buf), start);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ByteBuffer b = ByteBuffer.wrap(buf);
        while (b.remaining() > 0) {
            int klen = b.getInt();
            int vlen = b.getInt();
            int cmp = compareAt(buf, b.position(), klen, key);
            if (cmp == 0) {
                if (vlen == -1) return Memtable.TOMBSTONE;
                byte[] v = new byte[vlen];
                b.position(b.position() + klen);
                b.get(v);
                return v;
            }
            if (cmp > 0) return null; // sorted: passed the key
            b.position(b.position() + klen + (vlen == -1 ? 0 : vlen));
        }
        return null;
    }

    /** Iterator over entries with the given prefix (empty prefix = full scan), ascending; includes tombstones. */
    SegmentIterator iterate(byte[] prefix) {
        long start = 4; // after magic
        if (prefix.length > 0) {
            int block = floorBlock(prefix);
            if (block >= 0) start = sparseOffsets[block];
        }
        return new SegmentIterator(start, prefix);
    }

    /** Unbounded ascending iterator positioned near {@code startKey} (may emit a few earlier keys; caller skips). */
    SegmentIterator iterateFrom(byte[] startKey) {
        long start = 4;
        if (startKey.length > 0) {
            int block = floorBlock(startKey);
            if (block >= 0) start = sparseOffsets[block];
        }
        return new SegmentIterator(start, io.chronodim.storage.util.Bytes.EMPTY);
    }

    /** Greatest sparse block whose first key is <= key; -1 when key precedes everything. */
    private int floorBlock(byte[] key) {
        int lo = 0, hi = sparseKeys.length - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Bytes.compare(sparseKeys[mid], key) <= 0) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        // A key smaller than the first sparse key can still live in block 0
        // only if block 0 starts before it — it cannot: sparse[0] is the segment's
        // first key. For prefix scans we still start at block 0.
        return ans;
    }

    private static int compareAt(byte[] buf, int off, int len, byte[] key) {
        int n = Math.min(len, key.length);
        for (int i = 0; i < n; i++) {
            int c = (buf[off + i] & 0xFF) - (key[i] & 0xFF);
            if (c != 0) return c;
        }
        return len - key.length;
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) throw new CorruptionException("unexpected EOF reading segment at " + p);
            p += n;
        }
    }

    /** Streaming reader with its own buffer; safe to use concurrently with point reads. */
    final class SegmentIterator implements java.util.Iterator<LsmEntry>, AutoCloseable {
        private static final int BUF = 256 << 10;
        private final byte[] prefix;
        private long filePos;
        private byte[] buf = new byte[BUF];
        private int bufLen;
        private int bufOff;
        private LsmEntry next;
        private boolean done;

        SegmentIterator(long startPos, byte[] prefix) {
            this.filePos = startPos;
            this.prefix = prefix;
            retain();
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
            if (done) return;
            try {
                while (true) {
                    if (!ensure(8)) { finish(); return; }
                    int klen = readInt();
                    int vlen = readInt();
                    int need = klen + (vlen == -1 ? 0 : vlen);
                    if (!ensure(need)) throw new CorruptionException("segment truncated mid-entry: " + path);
                    byte[] k = new byte[klen];
                    System.arraycopy(buf, bufOff, k, 0, klen);
                    bufOff += klen;
                    if (prefix.length > 0) {
                        if (!Bytes.hasPrefix(k, prefix)) {
                            if (Bytes.compare(k, prefix) > 0) { finish(); return; }
                            bufOff += vlen == -1 ? 0 : vlen; // still before prefix range
                            continue;
                        }
                    }
                    byte[] v;
                    if (vlen == -1) {
                        v = Memtable.TOMBSTONE;
                    } else {
                        v = new byte[vlen];
                        System.arraycopy(buf, bufOff, v, 0, vlen);
                        bufOff += vlen;
                    }
                    next = new LsmEntry(k, v);
                    return;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** Ensures n bytes available in buf starting at bufOff; false at clean end-of-data. */
        private boolean ensure(int n) throws IOException {
            long consumedAbs = filePos + bufOff;
            if (consumedAbs >= dataEnd && bufLen - bufOff == 0) return false;
            if (bufLen - bufOff >= n) {
                // Never read past dataEnd: entries end exactly there.
                return consumedAbs + n <= dataEnd;
            }
            // compact + refill
            int rem = bufLen - bufOff;
            System.arraycopy(buf, bufOff, buf, 0, rem);
            filePos += bufOff;
            bufOff = 0;
            bufLen = rem;
            if (n > buf.length) buf = java.util.Arrays.copyOf(buf, Integer.highestOneBit(n) * 2);
            long maxRead = dataEnd - (filePos + bufLen);
            while (bufLen < n && maxRead > 0) {
                int want = (int) Math.min(buf.length - bufLen, maxRead);
                int r = channel.read(ByteBuffer.wrap(buf, bufLen, want), filePos + bufLen);
                if (r < 0) throw new CorruptionException("unexpected EOF in segment " + path);
                bufLen += r;
                maxRead -= r;
            }
            return bufLen - bufOff >= n;
        }

        private int readInt() {
            int v = ((buf[bufOff] & 0xFF) << 24) | ((buf[bufOff + 1] & 0xFF) << 16)
                    | ((buf[bufOff + 2] & 0xFF) << 8) | (buf[bufOff + 3] & 0xFF);
            bufOff += 4;
            return v;
        }

        private void finish() {
            if (!done) {
                done = true;
                release();
            }
        }

        @Override
        public void close() {
            finish();
        }
    }
}
