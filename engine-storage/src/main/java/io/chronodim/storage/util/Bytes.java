package io.chronodim.storage.util;

/** Byte-array helpers. Keys sort by unsigned lexicographic order everywhere. */
public final class Bytes {
    private Bytes() {}

    public static final byte[] EMPTY = new byte[0];

    public static int compare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int c = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (c != 0) return c;
        }
        return a.length - b.length;
    }

    public static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    public static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) len += p.length;
        byte[] out = new byte[len];
        int o = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, o, p.length);
            o += p.length;
        }
        return out;
    }

    /** Smallest key strictly greater than every key having {@code prefix}, or null if none. */
    public static byte[] prefixEnd(byte[] prefix) {
        byte[] end = prefix.clone();
        for (int i = end.length - 1; i >= 0; i--) {
            if (end[i] != (byte) 0xFF) {
                end[i]++;
                return java.util.Arrays.copyOf(end, i + 1);
            }
        }
        return null;
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        return sb.toString();
    }
}
