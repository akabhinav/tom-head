package io.chronodim.storage.lsm;

/** Internal merge entry; value == Memtable.TOMBSTONE (identity) marks a delete. */
record LsmEntry(byte[] key, byte[] value) {
    boolean tombstone() {
        return value == Memtable.TOMBSTONE;
    }
}
