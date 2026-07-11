package io.chronodim.storage.lsm;

import io.chronodim.storage.util.XxHash64;

/** Blocked-free classic bloom filter, ~10 bits/key, k=7, double hashing over xxHash64. */
final class BloomFilter {
    private static final long SEED1 = 0x51_7c_c1_b7_27_22_0a_95L;
    private static final long SEED2 = 0x27_22_0a_95_51_7c_c1_b7L;

    final long[] bits;
    final int numHashes;

    private BloomFilter(long[] bits, int numHashes) {
        this.bits = bits;
        this.numHashes = numHashes;
    }

    static BloomFilter create(long expectedKeys) {
        long bitCount = Math.max(64, expectedKeys * 10);
        int words = (int) Math.min(Integer.MAX_VALUE - 8, (bitCount + 63) / 64);
        return new BloomFilter(new long[words], 7);
    }

    static BloomFilter from(long[] bits, int numHashes) {
        return new BloomFilter(bits, numHashes);
    }

    void add(byte[] key) {
        long h1 = XxHash64.hash(key, 0, key.length, SEED1);
        long h2 = XxHash64.hash(key, 0, key.length, SEED2);
        long m = (long) bits.length * 64;
        for (int i = 0; i < numHashes; i++) {
            long bit = Long.remainderUnsigned(h1 + (long) i * h2, m);
            bits[(int) (bit >>> 6)] |= 1L << (bit & 63);
        }
    }

    boolean mightContain(byte[] key) {
        long h1 = XxHash64.hash(key, 0, key.length, SEED1);
        long h2 = XxHash64.hash(key, 0, key.length, SEED2);
        long m = (long) bits.length * 64;
        for (int i = 0; i < numHashes; i++) {
            long bit = Long.remainderUnsigned(h1 + (long) i * h2, m);
            if ((bits[(int) (bit >>> 6)] & (1L << (bit & 63))) == 0) return false;
        }
        return true;
    }
}
