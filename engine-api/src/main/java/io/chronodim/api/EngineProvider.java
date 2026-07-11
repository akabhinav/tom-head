package io.chronodim.api;

import java.nio.file.Path;

/** SPI implemented by engine-core; discovered via {@link java.util.ServiceLoader}. */
public interface EngineProvider {
    Engine open(Path dataDir, EngineOptions options);
}
