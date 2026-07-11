package io.chronodim.durability;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** Filesystem-backed object store (works against any mounted bucket). Writes are atomic (tmp + rename). */
public final class LocalFsObjectStore implements ObjectStore {

    private final Path root;

    public LocalFsObjectStore(Path root) {
        this.root = root;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path resolve(String key) {
        Path p = root.resolve(key).normalize();
        if (!p.startsWith(root)) throw new IllegalArgumentException("key escapes store root: " + key);
        return p;
    }

    @Override
    public void put(String key, Path file) {
        try {
            Path target = resolve(key);
            Files.createDirectories(target.getParent());
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            Files.copy(file, tmp, StandardCopyOption.REPLACE_EXISTING);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("object put failed: " + key, e);
        }
    }

    @Override
    public void putBytes(String key, byte[] content) {
        try {
            Path target = resolve(key);
            Files.createDirectories(target.getParent());
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            Files.write(tmp, content);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("object put failed: " + key, e);
        }
    }

    @Override
    public InputStream get(String key) throws IOException {
        return Files.newInputStream(resolve(key));
    }

    @Override
    public List<String> list(String prefix) {
        Path dir = root;
        try {
            if (!Files.exists(dir)) return List.of();
            List<String> out = new ArrayList<>();
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path p : walk.toList()) {
                    if (Files.isDirectory(p) || p.getFileName().toString().endsWith(".tmp")) continue;
                    String key = root.relativize(p).toString().replace('\\', '/');
                    if (key.startsWith(prefix)) out.add(key);
                }
            }
            Collections.sort(out);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean exists(String key) {
        return Files.exists(resolve(key));
    }

    @Override
    public long size(String key) {
        try {
            return Files.size(resolve(key));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
