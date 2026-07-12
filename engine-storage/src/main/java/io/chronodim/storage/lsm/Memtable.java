package io.chronodim.storage.lsm;

import io.chronodim.storage.util.Bytes;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory sorted write buffer. Deletes are stored as the identity-compared
 * {@link #TOMBSTONE} sentinel so they mask older segment entries during merges.
 */
final class Memtable {
    /** Identity-compared delete marker (only this exact instance means "deleted"). */
    static final byte[] TOMBSTONE = new byte[0];

    final ConcurrentNavigableMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(Bytes::compare);
    private final AtomicLong bytes = new AtomicLong();
    private volatile long maxTxn = -1;

    void put(byte[] key, byte[] value, long txnId) {
        byte[] prev = map.put(key, value);
        long delta = key.length + (value == TOMBSTONE ? 0 : value.length) + 48;
        if (prev != null) delta -= (prev == TOMBSTONE ? 0 : prev.length) + key.length + 48;
        bytes.addAndGet(delta);
        if (txnId > maxTxn) maxTxn = txnId;
    }

    /** Raw lookup: null = absent, TOMBSTONE (identity) = deleted here. */
    byte[] getRaw(byte[] key) {
        return map.get(key);
    }

    long approximateBytes() {
        return bytes.get();
    }

    long maxTxn() {
        return maxTxn;
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int count() {
        return map.size();
    }

    /** Ascending iterator over [from, prefix-end); tombstones included. */
    Iterator<Map.Entry<byte[], byte[]>> range(byte[] prefix) {
        byte[] end = Bytes.prefixEnd(prefix);
        ConcurrentNavigableMap<byte[], byte[]> sub =
                end == null ? map.tailMap(prefix, true) : map.subMap(prefix, true, end, false);
        return sub.entrySet().iterator();
    }

    Iterator<Map.Entry<byte[], byte[]>> all() {
        return map.entrySet().iterator();
    }

    /** Ascending iterator from {@code start} (inclusive) to the end; tombstones included. */
    Iterator<Map.Entry<byte[], byte[]>> from(byte[] start) {
        return map.tailMap(start, true).entrySet().iterator();
    }
}
