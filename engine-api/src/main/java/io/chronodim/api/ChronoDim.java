package io.chronodim.api;

import java.nio.file.Path;
import java.util.ServiceLoader;

/**
 * Entry point (R-API-1):
 * <pre>{@code
 * try (Engine engine = ChronoDim.open(Path.of("/data/dims"), EngineOptions.defaults())) {
 *     engine.apply(ApplyBatch.single("load-2026-07-11", "customer", rows));
 * }
 * }</pre>
 */
public final class ChronoDim {
    private ChronoDim() {}

    public static Engine open(Path dataDir) {
        return open(dataDir, EngineOptions.defaults());
    }

    public static Engine open(Path dataDir, EngineOptions options) {
        for (EngineProvider p : ServiceLoader.load(EngineProvider.class)) {
            return p.open(dataDir, options);
        }
        throw new ChronoDimException("no ChronoDim EngineProvider on the classpath (add the engine-core module)");
    }
}
