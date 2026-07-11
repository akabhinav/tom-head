package io.chronodim.api;

/** Invalid or incompatible table/engine configuration (R-CFG-3). */
public class ConfigException extends ChronoDimException {
    public ConfigException(String message) { super(message); }
    public ConfigException(String message, Throwable cause) { super(message, cause); }
}
