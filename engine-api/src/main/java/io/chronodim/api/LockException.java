package io.chronodim.api;

/** Another live writer owns the data directory (R-DUR-4 / NG2). */
public class LockException extends ChronoDimException {
    public LockException(String message) { super(message); }
}
