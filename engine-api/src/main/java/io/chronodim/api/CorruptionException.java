package io.chronodim.api;

/** On-disk state failed a checksum or structural invariant. Recovery refuses to guess. */
public class CorruptionException extends ChronoDimException {
    public CorruptionException(String message) { super(message); }
    public CorruptionException(String message, Throwable cause) { super(message, cause); }
}
