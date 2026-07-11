package io.chronodim.core.wal;

/** WAL record constants (R-WAL-2, frozen). */
public final class Wal {
    private Wal() {}

    public static final byte RECORD_VERSION = 1;

    public static final byte TXN_COMMIT = 1;
    public static final byte BACKFILL_BEGIN = 2;
    public static final byte BACKFILL_END = 3;
    public static final byte SNAPSHOT_MARK = 4;
    public static final byte CONFIG_CHANGE = 5;
    public static final byte PUBLISH_MARK = 6;

    /** [u32 crc32c][u32 len] precede every record; len covers version+type+txn+payload. */
    public static final int FRAME_HEADER = 8;
    public static final int RECORD_FIXED = 1 + 1 + 8;

    public static final String SEGMENT_PREFIX = "wal-";
    public static final String SEGMENT_SUFFIX = ".log";

    public static String segmentName(long startTxn) {
        return SEGMENT_PREFIX + String.format("%016x", startTxn) + SEGMENT_SUFFIX;
    }

    public static long segmentStartTxn(String fileName) {
        return Long.parseUnsignedLong(
                fileName.substring(SEGMENT_PREFIX.length(), fileName.length() - SEGMENT_SUFFIX.length()), 16);
    }
}
