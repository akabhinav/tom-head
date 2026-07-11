package io.chronodim.storage.rocks;

import io.chronodim.api.ChronoDimException;
import io.chronodim.api.CorruptionException;
import io.chronodim.storage.AtomicBatch;
import io.chronodim.storage.CloseableKvIterator;
import io.chronodim.storage.KV;
import io.chronodim.storage.KvSnapshot;
import io.chronodim.storage.StorageEngine;
import io.chronodim.storage.util.Bytes;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompressionType;
import org.rocksdb.EnvOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Default {@link StorageEngine} backend: RocksDB via RocksJava (R-STORE tech
 * decision). Only this package may import {@code org.rocksdb} — enforced by
 * {@code StorageIsolationTest}.
 *
 * <p>Recovery contract (R-STORE-4): RocksDB's own WAL is <b>disabled</b>; the
 * engine WAL above is the durability authority. Before every memtable flush a
 * watermark key is written recording the highest engine txn, so after a crash
 * {@link #durableTxn()} reflects exactly what the SSTs contain and the engine
 * replays its WAL from there.
 */
public final class RocksDbStorageEngine implements StorageEngine {

    /** Internal watermark key: 0xFF prefix keeps it outside every engine keyspace. */
    private static final byte[] DURABLE_TXN_KEY = {(byte) 0xFF, 'd', 'u', 'r', 'a', 'b', 'l', 'e'};

    private final Path dir;
    private final Options options;
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private volatile long lastWrittenTxn = -1;
    private volatile boolean closed;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDbStorageEngine(Path dir, long memtableBytes, long blockCacheBytes) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        BlockBasedTableConfig table = new BlockBasedTableConfig()
                .setBlockCache(new LRUCache(blockCacheBytes))
                .setFilterPolicy(new BloomFilter(10, false))
                .setWholeKeyFiltering(true)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);
        this.options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(memtableBytes)
                .setMaxWriteBufferNumber(4)
                .setTableFormatConfig(table)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setMaxBackgroundJobs(4)
                .setBytesPerSync(1 << 20)
                .setAvoidUnnecessaryBlockingIO(true);
        this.writeOptions = new WriteOptions().setDisableWAL(true); // engine WAL is authoritative
        try {
            this.db = RocksDB.open(options, dir.toString());
        } catch (RocksDBException e) {
            throw new ChronoDimException("cannot open RocksDB at " + dir, e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb get failed", e);
        }
    }

    @Override
    public List<byte[]> multiGet(List<byte[]> keys) {
        if (keys.isEmpty()) return List.of();
        try {
            return db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb multiGet failed", e);
        }
    }

    @Override
    public CloseableKvIterator prefixScan(byte[] prefix) {
        return iterate(prefix, new ReadOptions());
    }

    @Override
    public void write(AtomicBatch batch, long txnId) {
        checkOpen();
        try (WriteBatch wb = new WriteBatch()) {
            for (AtomicBatch.Mutation m : batch.mutations()) {
                switch (m) {
                    case AtomicBatch.Put p -> wb.put(p.key(), p.value());
                    case AtomicBatch.Delete d -> wb.delete(d.key());
                }
            }
            db.write(writeOptions, wb);
            if (txnId > lastWrittenTxn) lastWrittenTxn = txnId;
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb write failed", e);
        }
    }

    @Override
    public KvSnapshot snapshot() {
        checkOpen();
        Snapshot snap = db.getSnapshot();
        return new KvSnapshot() {
            private volatile boolean released;

            @Override
            public byte[] get(byte[] key) {
                try (ReadOptions ro = new ReadOptions().setSnapshot(snap)) {
                    return db.get(ro, key);
                } catch (RocksDBException e) {
                    throw new ChronoDimException("rocksdb snapshot get failed", e);
                }
            }

            @Override
            public CloseableKvIterator prefixScan(byte[] prefix) {
                return iterate(prefix, new ReadOptions().setSnapshot(snap));
            }

            @Override
            public void close() {
                if (!released) {
                    released = true;
                    db.releaseSnapshot(snap);
                }
            }
        };
    }

    @Override
    public void ingestSorted(Iterator<KV> sortedKvs, long txnId, long expectedKeys) {
        checkOpen();
        Path sst = dir.resolve("ingest-" + txnId + ".sst");
        try (EnvOptions env = new EnvOptions();
             Options opts = new Options();
             SstFileWriter writer = new SstFileWriter(env, opts)) {
            writer.open(sst.toString());
            long n = 0;
            while (sortedKvs.hasNext()) {
                KV kv = sortedKvs.next();
                writer.put(kv.key(), kv.value());
                n++;
            }
            if (n == 0) {
                writer.close();
                Files.deleteIfExists(sst);
            } else {
                writer.finish();
                try (IngestExternalFileOptions ing = new IngestExternalFileOptions()
                        .setMoveFiles(true)
                        .setSnapshotConsistency(true)) {
                    db.ingestExternalFile(List.of(sst.toString()), ing);
                }
            }
            if (txnId > lastWrittenTxn) lastWrittenTxn = txnId;
            flush(); // makes durableTxn cover the ingest immediately
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb ingest failed", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void flush() {
        checkOpen();
        try {
            long watermark = lastWrittenTxn;
            db.put(writeOptions, DURABLE_TXN_KEY, Long.toString(watermark).getBytes(StandardCharsets.US_ASCII));
            try (FlushOptions fo = new FlushOptions().setWaitForFlush(true)) {
                db.flush(fo);
            }
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb flush failed", e);
        }
    }

    @Override
    public void checkpoint(Path targetDir) {
        checkOpen();
        flush();
        try (Checkpoint cp = Checkpoint.create(db)) {
            Files.createDirectories(targetDir.getParent() == null ? targetDir : targetDir.getParent());
            // RocksDB requires the checkpoint directory to not exist.
            if (Files.exists(targetDir)) {
                throw new ChronoDimException("checkpoint target already exists: " + targetDir);
            }
            cp.createCheckpoint(targetDir.toString());
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb checkpoint failed", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long durableTxn() {
        // The durable watermark is what the *persistent* state says, not the live one:
        // read it back from the DB (it was written right before the last flush).
        // After open() with no flushes yet, that is exactly the recovered value.
        byte[] v = get(DURABLE_TXN_KEY);
        if (v == null) return -1;
        long stored = Long.parseLong(new String(v, StandardCharsets.US_ASCII));
        return stored;
    }

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("backend", "rocksdb");
        for (String p : List.of("rocksdb.estimate-num-keys", "rocksdb.block-cache-usage",
                "rocksdb.estimate-live-data-size", "rocksdb.num-files-at-level0")) {
            try {
                m.put(p.replace("rocksdb.", "").replace('-', '_'), db.getProperty(p));
            } catch (RocksDBException ignored) {
                // property not available on this version
            }
        }
        m.put("durable_txn", durableTxn());
        return m;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            long watermark = lastWrittenTxn;
            db.put(writeOptions, DURABLE_TXN_KEY, Long.toString(watermark).getBytes(StandardCharsets.US_ASCII));
            try (FlushOptions fo = new FlushOptions().setWaitForFlush(true)) {
                db.flush(fo);
            }
        } catch (RocksDBException e) {
            throw new ChronoDimException("rocksdb close-flush failed", e);
        } finally {
            writeOptions.close();
            db.close();
            options.close();
        }
    }

    private void checkOpen() {
        if (closed) throw new ChronoDimException("storage engine is closed");
    }

    private CloseableKvIterator iterate(byte[] prefix, ReadOptions ro) {
        byte[] end = Bytes.prefixEnd(prefix);
        if (end != null) ro.setIterateUpperBound(new Slice(end));
        RocksIterator it = db.newIterator(ro);
        if (prefix.length == 0) it.seekToFirst();
        else it.seek(prefix);
        return new CloseableKvIterator() {
            private boolean done;

            @Override
            public boolean hasNext() {
                if (done) return false;
                if (!it.isValid()) {
                    checkStatus();
                    return false;
                }
                if (prefix.length > 0 && !Bytes.hasPrefix(it.key(), prefix)) return false;
                return true;
            }

            @Override
            public KV next() {
                if (!hasNext()) throw new NoSuchElementException();
                KV kv = new KV(it.key(), it.value());
                it.next();
                return kv;
            }

            private void checkStatus() {
                try {
                    it.status();
                } catch (RocksDBException e) {
                    throw new CorruptionException("rocksdb iterator error", e);
                }
            }

            @Override
            public void close() {
                done = true;
                it.close();
                ro.close();
            }
        };
    }
}
