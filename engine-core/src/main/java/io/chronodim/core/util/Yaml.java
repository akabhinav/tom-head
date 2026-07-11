package io.chronodim.core.util;

import io.chronodim.api.ConfigException;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.util.Map;

/**
 * YAML loading for table/engine configs (R-CFG-1). Backed by SnakeYAML with the
 * safe constructor (no arbitrary object instantiation). JSON documents are valid
 * input too, so {@code table create} accepts either format.
 */
public final class Yaml {
    private Yaml() {}

    public static Object parse(String text) {
        try {
            LoaderOptions opts = new LoaderOptions();
            opts.setAllowDuplicateKeys(false);
            org.yaml.snakeyaml.Yaml y = new org.yaml.snakeyaml.Yaml(new SafeConstructor(opts));
            return y.load(text);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("YAML parse error: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseMap(String text) {
        Object v = parse(text);
        if (v == null) return new java.util.LinkedHashMap<>();
        if (!(v instanceof Map)) throw new ConfigException("expected a YAML mapping at top level, got " + v.getClass().getSimpleName());
        return (Map<String, Object>) v;
    }
}
