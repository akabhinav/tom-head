package io.chronodim.api;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Mints numeric (BIGINT) run ids that are unique for the next ~270 years:
 *
 * <pre>
 *   63 bits = [ 43-bit milliseconds since 2026-01-01 | 8-bit node | 12-bit sequence ]
 * </pre>
 *
 * <ul>
 *   <li>43 bits of milliseconds cover ~278 years from the custom epoch.</li>
 *   <li>8 node bits separate up to 256 concurrent minting processes (derived
 *       from hostname+pid by default; set {@code chronodim.node.id} to pin it).</li>
 *   <li>12 sequence bits allow 4096 ids per millisecond per process; the
 *       sequence spins onto the next millisecond when exhausted, so a single
 *       process can never repeat an id — even minting millions per run.</li>
 * </ul>
 *
 * Ids are positive, strictly increasing per process, sort by creation time,
 * and always fit a signed 64-bit BIGINT column. Uniqueness across processes
 * relies on distinct node bits; when two uncoordinated processes do collide on
 * both node bits and millisecond, the engine's duplicate-load_id check still
 * refuses the second batch — the id space is the first line of defense, the
 * idempotency ledger is the guarantee.
 */
public final class RunId {

    private static final long EPOCH_MS = 1767225600000L; // 2026-01-01T00:00:00Z
    private static final int NODE_BITS = 8;
    private static final int SEQ_BITS = 12;
    private static final long NODE;
    private static final AtomicLong LAST = new AtomicLong(); // (ms << SEQ_BITS) | seq

    static {
        long node;
        String pinned = System.getProperty("chronodim.node.id");
        if (pinned != null) {
            node = Long.parseLong(pinned);
            if (node < 0 || node >= (1 << NODE_BITS)) {
                throw new ConfigException("chronodim.node.id must be 0.." + ((1 << NODE_BITS) - 1));
            }
        } else {
            String host;
            try {
                host = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = "unknown";
            }
            node = Math.floorMod((host + "/" + ProcessHandle.current().pid()).hashCode(), 1 << NODE_BITS);
        }
        NODE = node;
    }

    private RunId() {}

    /** Next unique run id. Thread-safe; never repeats within this process. */
    public static long next() {
        while (true) {
            long nowMs = Math.max(0, System.currentTimeMillis() - EPOCH_MS);
            long last = LAST.get();
            long candidate;
            long lastMs = last >>> SEQ_BITS;
            if (nowMs > lastMs) {
                candidate = nowMs << SEQ_BITS;
            } else {
                // same (or rewound) clock millisecond: bump the sequence, spilling
                // into the next millisecond when the 4096/ms budget is exhausted —
                // monotonicity survives even a backwards NTP step.
                candidate = last + 1;
            }
            if (LAST.compareAndSet(last, candidate)) {
                long ms = candidate >>> SEQ_BITS;
                long seq = candidate & ((1 << SEQ_BITS) - 1);
                return (ms << (NODE_BITS + SEQ_BITS)) | (NODE << SEQ_BITS) | seq;
            }
        }
    }
}
