package io.chronodim.core.engine;

import io.chronodim.api.LockException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Single-writer lockfile (R-DUR-4, NG2). Holds an OS advisory lock for the
 * process lifetime and records pid/host for diagnostics. Stale-lock takeover
 * happens implicitly when the owning process died (the OS releases the lock);
 * a *live* holder always wins.
 */
final class EngineLock implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;
    private final Path path;

    EngineLock(Path dataDir) {
        this.path = dataDir.resolve("LOCK");
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileLock l;
            try {
                l = channel.tryLock();
            } catch (java.nio.channels.OverlappingFileLockException e) {
                l = null; // same-JVM second writer
            }
            if (l == null) {
                String holder = readHolder();
                channel.close();
                throw new LockException("data directory is locked by another live writer"
                        + (holder.isEmpty() ? "" : " (" + holder + ")") + ": " + path);
            }
            lock = l;
            String info = "pid=" + ProcessHandle.current().pid()
                    + " host=" + hostName()
                    + " started=" + ManagementFactory.getRuntimeMXBean().getStartTime();
            channel.truncate(0);
            channel.write(java.nio.ByteBuffer.wrap(info.getBytes(StandardCharsets.UTF_8)), 0);
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot acquire lock " + path, e);
        }
    }

    private String readHolder() {
        try {
            return Files.exists(path) ? Files.readString(path).strip() : "";
        } catch (IOException e) {
            return "";
        }
    }

    private static String hostName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    @Override
    public void close() {
        try {
            lock.release();
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
