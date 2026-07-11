package io.chronodim.storage.lsm;

import io.chronodim.api.CorruptionException;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Streams strictly-ascending unique keys into an on-disk sorted segment.
 *
 * <p>Segment file format v1 (frozen; see docs/formats.md):
 * <pre>
 * "CDS1"
 * entries:  [u32 klen][i32 vlen][key][value]      vlen == -1 → tombstone (no value bytes)
 * index:    [u64 entryCount][u32 sparseInterval][u32 sparseCount]
 *           sparseCount × ([u32 klen][key][u64 entryOffset])
 * bloom:    [u32 numWords][u32 numHashes][numWords × u64]
 * footer:   [u64 indexOff][u64 bloomOff][u64 entryCount][u8 version][u32 crc32c]["CDS1"]
 * </pre>
 * All integers big-endian. The footer CRC covers indexOff..version.
 */
final class SegmentWriter implements AutoCloseable {
    static final byte[] MAGIC = {'C', 'D', 'S', '1'};
    static final int SPARSE_INTERVAL = 64;
    static final int FOOTER_LEN = 8 + 8 + 8 + 1 + 4 + 4;
    static final byte FORMAT_VERSION = 1;

    private final FileOutputStream fos;
    private final OutputStream out;
    private final BloomFilter bloom;
    private final List<byte[]> sparseKeys = new ArrayList<>();
    private final List<Long> sparseOffsets = new ArrayList<>();
    private final byte[] intBuf = new byte[8];
    private final boolean fsync;

    private long position;
    private long entryCount;
    private byte[] lastKey;

    SegmentWriter(Path path, long expectedKeys, boolean fsync) {
        this.fsync = fsync;
        try {
            this.fos = new FileOutputStream(path.toFile());
            this.out = new BufferedOutputStream(fos, 1 << 20);
            this.bloom = BloomFilter.create(Math.max(1, expectedKeys));
            out.write(MAGIC);
            position = 4;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create segment " + path, e);
        }
    }

    void append(byte[] key, byte[] value, boolean tombstone) {
        if (lastKey != null && io.chronodim.storage.util.Bytes.compare(lastKey, key) >= 0) {
            throw new CorruptionException("segment writer: keys not strictly ascending");
        }
        lastKey = key;
        try {
            if (entryCount % SPARSE_INTERVAL == 0) {
                sparseKeys.add(key);
                sparseOffsets.add(position);
            }
            writeInt(key.length);
            writeInt(tombstone ? -1 : value.length);
            out.write(key);
            position += 8 + key.length;
            if (!tombstone) {
                out.write(value);
                position += value.length;
            }
            bloom.add(key);
            entryCount++;
        } catch (IOException e) {
            throw new UncheckedIOException("segment append failed", e);
        }
    }

    long entryCount() {
        return entryCount;
    }

    /** Writes index, bloom and footer, then fsyncs. Returns final file size. */
    long finish() {
        try {
            long indexOff = position;
            writeLong(entryCount);
            writeInt(SPARSE_INTERVAL);
            writeInt(sparseKeys.size());
            position += 16;
            for (int i = 0; i < sparseKeys.size(); i++) {
                byte[] k = sparseKeys.get(i);
                writeInt(k.length);
                out.write(k);
                writeLong(sparseOffsets.get(i));
                position += 12 + k.length;
            }
            long bloomOff = position;
            long[] words = bloom.bits;
            writeInt(words.length);
            writeInt(bloom.numHashes);
            position += 8;
            for (long w : words) {
                writeLong(w);
            }
            position += 8L * words.length;

            ByteBuffer footer = ByteBuffer.allocate(FOOTER_LEN);
            footer.putLong(indexOff).putLong(bloomOff).putLong(entryCount).put(FORMAT_VERSION);
            CRC32C crc = new CRC32C();
            crc.update(footer.array(), 0, 25);
            footer.putInt((int) crc.getValue());
            footer.put(MAGIC);
            out.write(footer.array());
            position += FOOTER_LEN;

            out.flush();
            if (fsync) fos.getFD().sync();
            return position;
        } catch (IOException e) {
            throw new UncheckedIOException("segment finish failed", e);
        }
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeInt(int v) throws IOException {
        intBuf[0] = (byte) (v >>> 24);
        intBuf[1] = (byte) (v >>> 16);
        intBuf[2] = (byte) (v >>> 8);
        intBuf[3] = (byte) v;
        out.write(intBuf, 0, 4);
    }

    private void writeLong(long v) throws IOException {
        intBuf[0] = (byte) (v >>> 56);
        intBuf[1] = (byte) (v >>> 48);
        intBuf[2] = (byte) (v >>> 40);
        intBuf[3] = (byte) (v >>> 32);
        intBuf[4] = (byte) (v >>> 24);
        intBuf[5] = (byte) (v >>> 16);
        intBuf[6] = (byte) (v >>> 8);
        intBuf[7] = (byte) v;
        out.write(intBuf, 0, 8);
    }
}
