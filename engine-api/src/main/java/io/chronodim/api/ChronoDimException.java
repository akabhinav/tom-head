package io.chronodim.api;

/** Base class for all ChronoDim errors. */
public class ChronoDimException extends RuntimeException {
    public ChronoDimException(String message) { super(message); }
    public ChronoDimException(String message, Throwable cause) { super(message, cause); }
}
