package io.chronodim.core.engine;

import io.chronodim.api.Engine;
import io.chronodim.api.EngineOptions;
import io.chronodim.api.EngineProvider;

import java.nio.file.Path;

/** ServiceLoader entry point backing {@link io.chronodim.api.ChronoDim#open}. */
public final class CoreEngineProvider implements EngineProvider {
    @Override
    public Engine open(Path dataDir, EngineOptions options) {
        return EngineImpl.open(dataDir, options);
    }
}
