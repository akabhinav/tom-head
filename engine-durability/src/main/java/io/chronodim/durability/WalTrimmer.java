package io.chronodim.durability;

import io.chronodim.core.wal.Wal;
import io.chronodim.core.wal.WalReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Point-in-time support for restore: drops WAL records with txn beyond the target. */
final class WalTrimmer {
    private WalTrimmer() {}

    static int count(Path walDir) {
        return WalReader.segments(walDir).size();
    }

    /** Truncates each segment at the first record with txnId > asOfTxn; deletes fully-beyond segments. */
    static void trimBeyond(Path walDir, long asOfTxn) {
        List<Path> segments = WalReader.segments(walDir);
        for (Path seg : segments) {
            long startTxn = Wal.segmentStartTxn(seg.getFileName().toString());
            if (startTxn > asOfTxn) {
                try {
                    Files.deleteIfExists(seg);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                continue;
            }
            // repairTail=true: a shipped active-segment chunk may legitimately end
            // mid-record; the torn tail is dropped exactly like crash recovery does.
            long[] cutAt = {-1};
            WalReader.scan(seg.getParent(), true, rec -> {
                if (!rec.segment().equals(seg.getFileName().toString())) return;
                if (rec.txnId() > asOfTxn && cutAt[0] < 0) cutAt[0] = rec.offset();
            });
            if (cutAt[0] >= 0) {
                try (var ch = java.nio.channels.FileChannel.open(seg, java.nio.file.StandardOpenOption.WRITE)) {
                    ch.truncate(cutAt[0]);
                    ch.force(true);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
