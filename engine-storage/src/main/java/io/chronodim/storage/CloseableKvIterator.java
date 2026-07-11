package io.chronodim.storage;

import java.util.Iterator;

public interface CloseableKvIterator extends Iterator<KV>, AutoCloseable {
    @Override
    void close();
}
