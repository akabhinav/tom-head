package io.chronodim.core.wal;

import io.chronodim.api.CorruptionException;
import io.chronodim.storage.AtomicBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Payload of TXN_COMMIT / CONFIG_CHANGE records (frozen):
 * <pre>
 * [u32 mutationCount]
 *   mutationCount × ([u8 op: 0=put 1=delete][u32 klen][u32 vlen][key][value if put])
 * [u32 manifestLen][manifest JSON utf8]
 * </pre>
 */
public final class TxnPayload {
    private TxnPayload() {}

    public static byte[] encode(AtomicBatch batch, String manifestJson) {
        byte[] mf = manifestJson.getBytes(StandardCharsets.UTF_8);
        int size = 4 + 4 + mf.length;
        for (AtomicBatch.Mutation m : batch.mutations()) {
            size += 9 + m.key().length;
            if (m instanceof AtomicBatch.Put p) size += p.value().length;
        }
        ByteBuffer b = ByteBuffer.allocate(size);
        b.putInt(batch.size());
        for (AtomicBatch.Mutation m : batch.mutations()) {
            switch (m) {
                case AtomicBatch.Put p -> {
                    b.put((byte) 0).putInt(p.key().length).putInt(p.value().length);
                    b.put(p.key()).put(p.value());
                }
                case AtomicBatch.Delete d -> {
                    b.put((byte) 1).putInt(d.key().length).putInt(0);
                    b.put(d.key());
                }
            }
        }
        b.putInt(mf.length).put(mf);
        return b.array();
    }

    public record Decoded(AtomicBatch batch, String manifestJson) {}

    public static Decoded decode(byte[] payload) {
        try {
            ByteBuffer b = ByteBuffer.wrap(payload);
            int count = b.getInt();
            AtomicBatch batch = new AtomicBatch();
            for (int i = 0; i < count; i++) {
                byte op = b.get();
                int klen = b.getInt();
                int vlen = b.getInt();
                byte[] key = new byte[klen];
                b.get(key);
                if (op == 0) {
                    byte[] value = new byte[vlen];
                    b.get(value);
                    batch.put(key, value);
                } else if (op == 1) {
                    batch.delete(key);
                } else {
                    throw new CorruptionException("bad mutation op " + op + " in WAL txn payload");
                }
            }
            byte[] mf = new byte[b.getInt()];
            b.get(mf);
            return new Decoded(batch, new String(mf, StandardCharsets.UTF_8));
        } catch (java.nio.BufferUnderflowException e) {
            throw new CorruptionException("truncated WAL txn payload", e);
        }
    }
}
