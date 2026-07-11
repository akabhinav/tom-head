package io.chronodim.core.wal;

import io.chronodim.api.ChronoDimException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Segmented, checksummed WAL writer with group commit (R-WAL-1/2/3).
 *
 * <p>Every append gets a monotonically increasing sequence number. A dedicated
 * syncer thread batches fsyncs: callers block in {@link #awaitDurable(long)} until
 * their sequence is covered. Under concurrency many transactions share one fsync
 * (window {@code groupWindowMicros} / {@code groupMaxTxns}, whichever first).
 */
public final class WalWriter implements AutoCloseable {

    private final Path dir;
    private final long segmentBytes;
    private final boolean fsync;
    private final long groupWindowNanos;
    private final int groupMaxTxns;

    private final Object lock = new Object();
    private FileChannel channel;
    private Path currentSegment;
    private long segmentSize;
    private long appendSeq;
    private long syncedSeq;
    private long fsyncCount;
    private long lastFsyncNanos;
    private boolean closed;
    private final Thread syncer;

    public WalWriter(Path dir, long segmentBytes, boolean fsync, long groupWindowMicros, int groupMaxTxns, long nextTxnHint) {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.fsync = fsync;
        this.groupWindowNanos = groupWindowMicros * 1_000L;
        this.groupMaxTxns = groupMaxTxns;
        try {
            Files.createDirectories(dir);
            Path latest = latestSegment(dir);
            if (latest != null) {
                currentSegment = latest;
                channel = FileChannel.open(latest, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                segmentSize = channel.size();
            } else {
                roll(nextTxnHint);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open WAL", e);
        }
        this.syncer = new Thread(this::syncLoop, "chronodim-wal-syncer");
        this.syncer.setDaemon(true);
        this.syncer.start();
    }

    static Path latestSegment(Path dir) throws IOException {
        Path best = null;
        long bestTxn = -1;
        try (var ds = Files.newDirectoryStream(dir, Wal.SEGMENT_PREFIX + "*" + Wal.SEGMENT_SUFFIX)) {
            for (Path p : ds) {
                long t = Wal.segmentStartTxn(p.getFileName().toString());
                if (t > bestTxn) {
                    bestTxn = t;
                    best = p;
                }
            }
        }
        return best;
    }

    /** Position of an appended record for audit manifests. */
    public record Appended(long seq, String segment, long offset, long endOffset) {}

    /**
     * Lets the caller build the payload knowing the exact record position — audit
     * manifests embed their own WAL coordinates (R-APPLY-7).
     */
    public interface PayloadFactory {
        byte[] create(String segment, long offset);
    }

    public Appended append(byte type, long txnId, byte[] payload) {
        return append(type, txnId, (seg, off) -> payload);
    }

    public Appended append(byte type, long txnId, PayloadFactory factory) {
        synchronized (lock) {
            if (closed) throw new ChronoDimException("WAL closed");
            try {
                if (segmentSize >= segmentBytes) {
                    forceLocked();
                    roll(txnId);
                }
                byte[] payload = factory.create(currentSegment.getFileName().toString(), segmentSize);
                return appendLocked(type, txnId, payload);
            } catch (IOException e) {
                throw new UncheckedIOException("WAL append failed", e);
            }
        }
    }

    /** Must hold {@code lock}. */
    private Appended appendLocked(byte type, long txnId, byte[] payload) throws IOException {
        int len = Wal.RECORD_FIXED + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(Wal.FRAME_HEADER + len);
        buf.position(Wal.FRAME_HEADER);
        buf.put(Wal.RECORD_VERSION).put(type).putLong(txnId).put(payload);
        buf.flip();
        buf.position(4);
        buf.putInt(len);
        CRC32C crc = new CRC32C();
        crc.update(buf.array(), 4, len + 4);
        buf.putInt(0, (int) crc.getValue());
        buf.position(0);

        long offset = segmentSize;
        while (buf.hasRemaining()) {
            segmentSize += channel.write(buf);
        }
        appendSeq++;
        lock.notifyAll();
        return new Appended(appendSeq, currentSegment.getFileName().toString(), offset, segmentSize);
    }

    /** Blocks until the given append sequence is fsync-durable (group commit). */
    public void awaitDurable(long seq) {
        if (!fsync) return;
        synchronized (lock) {
            while (syncedSeq < seq) {
                if (closed) throw new ChronoDimException("WAL closed while awaiting durability");
                try {
                    lock.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ChronoDimException("interrupted awaiting WAL durability", e);
                }
            }
        }
    }

    private void syncLoop() {
        while (true) {
            long target;
            synchronized (lock) {
                while (!closed && appendSeq == syncedSeq) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (closed && appendSeq == syncedSeq) return;
                target = appendSeq;
            }
            // Group window: let more commits pile onto this fsync.
            if (groupWindowNanos > 0) {
                long deadline = System.nanoTime() + groupWindowNanos;
                while (System.nanoTime() < deadline) {
                    synchronized (lock) {
                        if (appendSeq - syncedSeq >= groupMaxTxns || closed) break;
                        target = appendSeq;
                    }
                    Thread.onSpinWait();
                }
            }
            synchronized (lock) {
                target = appendSeq;
                try {
                    if (fsync) {
                        long t0 = System.nanoTime();
                        channel.force(false);
                        lastFsyncNanos = System.nanoTime() - t0;
                        fsyncCount++;
                    }
                    syncedSeq = target;
                    lock.notifyAll();
                } catch (IOException e) {
                    throw new UncheckedIOException("WAL fsync failed", e);
                }
            }
        }
    }

    /** Must hold {@code lock}. */
    private void forceLocked() throws IOException {
        if (fsync && channel != null) channel.force(false);
        syncedSeq = appendSeq;
        lock.notifyAll();
    }

    /** Must hold {@code lock} (or be called from the constructor). */
    private void roll(long startTxn) throws IOException {
        if (channel != null) channel.close();
        currentSegment = dir.resolve(Wal.segmentName(startTxn));
        // A segment name collision (same start txn after recovery truncation) appends.
        channel = FileChannel.open(currentSegment, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        segmentSize = channel.size();
    }

    public java.util.Map<String, Object> stats() {
        synchronized (lock) {
            var m = new java.util.LinkedHashMap<String, Object>();
            m.put("segment", currentSegment.getFileName().toString());
            m.put("segment_bytes", segmentSize);
            m.put("appends", appendSeq);
            m.put("fsyncs", fsyncCount);
            m.put("last_fsync_micros", lastFsyncNanos / 1_000);
            return m;
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) return;
            try {
                forceLocked();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            closed = true;
            lock.notifyAll();
        }
        try {
            syncer.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (lock) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
