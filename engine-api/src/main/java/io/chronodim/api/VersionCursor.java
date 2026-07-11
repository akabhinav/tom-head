package io.chronodim.api;

import java.util.Iterator;

/** Streaming, snapshot-consistent iterator over versions. Always close. */
public interface VersionCursor extends Iterator<Version>, AutoCloseable {
    @Override
    void close();
}
