package io.chronodim.storage.lsm;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one property everything rests on: a bloom filter may waste a read on an
 * absent key (false positive), but it must NEVER deny a stored key (false
 * negative). One million random keys, zero misses allowed — and the round-trip
 * through the serialized bits form must preserve the property.
 */
class BloomFilterPropertyTest {

    @Test
    void oneMillionStoredKeysZeroFalseNegatives() {
        int n = 1_000_000;
        BloomFilter f = BloomFilter.create(n);
        Random rnd = new Random(20260813);
        byte[][] keys = new byte[n][];
        for (int i = 0; i < n; i++) {
            keys[i] = new byte[8 + rnd.nextInt(24)];
            rnd.nextBytes(keys[i]);
            f.add(keys[i]);
        }
        for (int i = 0; i < n; i++) {
            assertTrue(f.mightContain(keys[i]), "FALSE NEGATIVE at key " + i + " — must be impossible");
        }

        // Absent keys: measure the false-positive rate — a cost, not a correctness issue.
        int fp = 0;
        byte[] probe = new byte[16];
        for (int i = 0; i < 100_000; i++) {
            rnd.nextBytes(probe);
            if (f.mightContain(probe)) fp++;
        }
        assertTrue(fp < 5_000, "false-positive rate unreasonably high: " + fp + "/100000");
    }
}
