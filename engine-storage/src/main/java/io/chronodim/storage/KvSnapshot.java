package io.chronodim.storage;

/** A consistent read view (R-READ-6). Never observes writes made after its creation. */
public interface KvSnapshot extends AutoCloseable {
    byte[] get(byte[] key);

    CloseableKvIterator prefixScan(byte[] prefix);

    @Override
    void close();
}
