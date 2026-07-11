package io.chronodim.testkit;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R-STORE-5: nothing above engine-storage may import org.rocksdb. Enforced by a
 * dependency-free source scan (same guarantee ArchUnit would give).
 */
class StorageIsolationTest {

    @Test
    void onlyEngineStorageImportsRocksDb() throws IOException {
        Path root = findRepoRoot();
        List<String> offenders = new ArrayList<>();
        for (String module : List.of("engine-api", "engine-core", "engine-export",
                "engine-durability", "engine-cli", "engine-bench", "engine-testkit")) {
            Path src = root.resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(src)) continue;
            try (Stream<Path> walk = Files.walk(src)) {
                for (Path p : walk.filter(f -> f.toString().endsWith(".java")).toList()) {
                    String content = Files.readString(p);
                    if (content.contains("org.rocksdb")) {
                        offenders.add(root.relativize(p).toString());
                    }
                }
            }
        }
        assertTrue(offenders.isEmpty(), "org.rocksdb leaked above engine-storage: " + offenders);
    }

    private static Path findRepoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("engine-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("cannot locate repository root");
        return p;
    }
}
