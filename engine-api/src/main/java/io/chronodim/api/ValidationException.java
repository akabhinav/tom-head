package io.chronodim.api;

/** Input data failed validation or type coercion. */
public class ValidationException extends ChronoDimException {
    public ValidationException(String message) { super(message); }
    public ValidationException(String message, Throwable cause) { super(message, cause); }
}
