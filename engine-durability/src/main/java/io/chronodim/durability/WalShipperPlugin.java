package io.chronodim.durability;

import io.chronodim.core.engine.EngineImpl;
import io.chronodim.core.engine.EnginePlugin;
import io.chronodim.core.wal.WalReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Continuous WAL shipping (R-DUR-1): every {@code walShipIntervalMillis} the
 * shipper uploads new WAL bytes to {@code <store>/wal/}. Closed segments upload
 * whole; the active segment ships incrementally in chunk files
 * ({@code <segment>.chunk-<offset>}) so worst-case RPO stays within seconds.
 * Restore concatenates chunks in offset order.
 */
public final class WalShipperPlugin implements EnginePlugin {

    private EngineImpl engine;
    private ObjectStore store;
    private Thread worker;
    private volatile boolean running;
    private volatile long lastShipMillis;
    private final Map<String, Long> shippedBytes = new HashMap<>();

    @Override
    public boolean attach(EngineImpl engine) {
        String uri = engine.options().objectStoreUri();
        if (uri == null || uri.isBlank()) return false;
        this.engine = engine;
        this.store = ObjectStore.open(uri);
        // Resume from what's already in the store.
        for (String key : store.list("wal/")) {
            String name = key.substring("wal/".length());
            if (name.contains(".chunk-")) {
                String seg = name.substring(0, name.indexOf(".chunk-"));
                long offset = Long.parseLong(name.substring(name.indexOf(".chunk-") + 7));
                long end = offset + store.size(key);
                shippedBytes.merge(seg, end, Math::max);
            } else {
                shippedBytes.merge(name, store.size(key), Math::max);
            }
        }
        running = true;
        worker = Thread.ofVirtual().name("chronodim-wal-shipper").start(this::loop);
        return true;
    }

    private void loop() {
        while (running) {
            try {
                Thread.sleep(engine.options().walShipIntervalMillis());
                shipOnce();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                System.err.println("[chronodim-shipper] ship cycle failed: " + e);
            }
        }
    }

    synchronized void shipOnce() {
        List<Path> segments = WalReader.segments(engine.walDir());
        for (int i = 0; i < segments.size(); i++) {
            Path seg = segments.get(i);
            String name = seg.getFileName().toString();
            boolean active = i == segments.size() - 1;
            long size;
            try {
                size = Files.size(seg);
            } catch (IOException e) {
                continue; // pruned concurrently
            }
            long already = shippedBytes.getOrDefault(name, 0L);
            if (size <= already && !(!active && already < size)) {
                if (!active && !store.exists("wal/" + name) && already >= size) {
                    // fully chunk-shipped closed segment: consolidate to a whole-file object
                    store.put("wal/" + name, seg);
                }
                continue;
            }
            if (!active) {
                store.put("wal/" + name, seg);
                shippedBytes.put(name, size);
            } else {
                shipChunk(seg, name, already, size);
            }
        }
        lastShipMillis = System.currentTimeMillis();
    }

    private void shipChunk(Path seg, String name, long from, long to) {
        if (to <= from) return;
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate((int) Math.min(to - from, 32 << 20));
            long pos = from;
            while (buf.hasRemaining() && pos < to) {
                int n = ch.read(buf, pos);
                if (n < 0) break;
                pos += n;
            }
            buf.flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            store.putBytes("wal/" + name + ".chunk-" + from, bytes);
            shippedBytes.put(name, from + bytes.length);
        } catch (IOException e) {
            throw new UncheckedIOException("chunk ship failed for " + name, e);
        }
    }

    @Override
    public synchronized boolean allowWalPrune(String segmentName, long maxTxnInSegment) {
        // Only fully-shipped segments may leave the local disk: the object-store
        // copy is the 10-year audit archive.
        Path seg = engine.walDir().resolve(segmentName);
        try {
            long size = Files.exists(seg) ? Files.size(seg) : 0;
            if (shippedBytes.getOrDefault(segmentName, 0L) >= size
                    && (store.exists("wal/" + segmentName) || size == 0)) {
                return true;
            }
            // One synchronous attempt so `snapshot --prune-wal` doesn't have to wait
            // for the next shipping tick.
            shipOnce();
            return shippedBytes.getOrDefault(segmentName, 0L) >= Files.size(seg)
                    && store.exists("wal/" + segmentName);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Map<String, Object> stats() {
        if (engine == null) return Map.of();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("wal_ship_age_ms", lastShipMillis == 0 ? -1 : System.currentTimeMillis() - lastShipMillis);
        return m;
    }

    @Override
    public void close() {
        running = false;
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            shipOnce(); // final drain: clean shutdown leaves RPO = 0
        }
    }
}
