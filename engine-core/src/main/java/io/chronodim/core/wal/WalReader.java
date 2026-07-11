package io.chronodim.core.wal;

import io.chronodim.api.CorruptionException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Sequential WAL scan for recovery, publishing and shipping (R-WAL-3).
 *
 * <p>Corruption policy: a corrupt or truncated record at the tail of the LAST
 * segment is truncated away (clean shutdown mid-write); corruption anywhere else
 * fails loudly with {@link CorruptionException}.
 */
public final class WalReader {

    public record Record(byte type, long txnId, byte[] payload, String segment, long offset, long endOffset) {}

    private WalReader() {}

    public static List<Path> segments(Path walDir) {
        if (!Files.isDirectory(walDir)) return List.of();
        try (var ds = Files.newDirectoryStream(walDir, Wal.SEGMENT_PREFIX + "*" + Wal.SEGMENT_SUFFIX)) {
            List<Path> out = new ArrayList<>();
            ds.forEach(out::add);
            out.sort(Comparator.comparing(p -> Wal.segmentStartTxn(p.getFileName().toString())));
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Scans every record in order, invoking the consumer. When {@code repairTail}
     * is true (recovery), a corrupt tail record in the last segment is truncated;
     * otherwise scanning is strictly read-only and stops with an exception.
     *
     * @return the highest txn id seen, or -1 when the WAL is empty.
     */
    public static long scan(Path walDir, boolean repairTail, Consumer<Record> consumer) {
        List<Path> segs = segments(walDir);
        long maxTxn = -1;
        for (int si = 0; si < segs.size(); si++) {
            Path seg = segs.get(si);
            boolean last = si == segs.size() - 1;
            maxTxn = Math.max(maxTxn, scanSegment(seg, last, repairTail, consumer));
        }
        return maxTxn;
    }

    private static long scanSegment(Path seg, boolean lastSegment, boolean repairTail, Consumer<Record> consumer) {
        String name = seg.getFileName().toString();
        long maxTxn = -1;
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            long size = ch.size();
            long pos = 0;
            ByteBuffer head = ByteBuffer.allocate(Wal.FRAME_HEADER);
            while (pos < size) {
                head.clear();
                if (size - pos < Wal.FRAME_HEADER) {
                    return handleBadTail(seg, lastSegment, repairTail, pos, "truncated frame header", maxTxn);
                }
                readFully(ch, head, pos);
                head.flip();
                int crcStored = head.getInt();
                int len = head.getInt();
                if (len < Wal.RECORD_FIXED || len > (64 << 20) || pos + Wal.FRAME_HEADER + len > size) {
                    return handleBadTail(seg, lastSegment, repairTail, pos, "bad record length " + len, maxTxn);
                }
                ByteBuffer body = ByteBuffer.allocate(4 + len);
                body.putInt(len);
                body.limit(4 + len);
                body.position(4);
                readFully(ch, body, pos + Wal.FRAME_HEADER);
                CRC32C crc = new CRC32C();
                crc.update(body.array(), 0, 4 + len);
                if ((int) crc.getValue() != crcStored) {
                    // A CRC failure with more bytes after the framed record is interior
                    // corruption, not a torn tail write — never silently truncate that.
                    if (pos + Wal.FRAME_HEADER + len < size) {
                        throw new CorruptionException("WAL " + name + " offset " + pos
                                + ": CRC mismatch with valid-length frame followed by more data — interior corruption"
                                + " (restore from snapshot + shipped WAL)");
                    }
                    return handleBadTail(seg, lastSegment, repairTail, pos, "CRC mismatch", maxTxn);
                }
                body.position(4);
                byte version = body.get();
                if (version != Wal.RECORD_VERSION) {
                    throw new CorruptionException("WAL " + name + " offset " + pos + ": unsupported record version " + version);
                }
                byte type = body.get();
                long txnId = body.getLong();
                byte[] payload = new byte[len - Wal.RECORD_FIXED];
                body.get(payload);
                long end = pos + Wal.FRAME_HEADER + len;
                consumer.accept(new Record(type, txnId, payload, name, pos, end));
                maxTxn = Math.max(maxTxn, txnId);
                pos = end;
            }
            return maxTxn;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read WAL segment " + seg, e);
        }
    }

    private static long handleBadTail(Path seg, boolean lastSegment, boolean repairTail,
                                      long pos, String why, long maxTxn) throws IOException {
        if (!lastSegment) {
            throw new CorruptionException("WAL " + seg.getFileName() + " offset " + pos + ": " + why
                    + " in a non-final segment — refusing to recover (restore from snapshot + shipped WAL)");
        }
        if (!repairTail) {
            throw new CorruptionException("WAL " + seg.getFileName() + " offset " + pos + ": " + why);
        }
        // Clean crash tail: truncate and continue (documented in R-WAL-3).
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.WRITE)) {
            ch.truncate(pos);
            ch.force(true);
        }
        return maxTxn;
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) throw new CorruptionException("unexpected EOF in WAL at " + p);
            p += n;
        }
    }
}
