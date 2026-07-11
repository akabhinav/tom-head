package io.chronodim.api;

/** Operation that produced a version (§5.2). */
public enum Op {
    INSERT((byte) 1), UPDATE((byte) 2), DELETE((byte) 3);

    public final byte code;
    Op(byte code) { this.code = code; }

    public static Op fromCode(byte b) {
        return switch (b) {
            case 1 -> INSERT;
            case 2 -> UPDATE;
            case 3 -> DELETE;
            default -> throw new CorruptionException("unknown op code " + b);
        };
    }
}
