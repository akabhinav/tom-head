package io.chronodim.storage;

/** A key/value pair. {@code value == null} inside iterators never happens (tombstones are filtered). */
public record KV(byte[] key, byte[] value) {}
