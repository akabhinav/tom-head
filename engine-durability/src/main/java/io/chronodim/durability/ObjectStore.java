package io.chronodim.durability;

import io.chronodim.api.ConfigException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Minimal object-store abstraction for WAL shipping and snapshots (R-DUR).
 * v1 ships a filesystem implementation (any mounted/NFS/FUSE bucket works);
 * an AWS SDK S3 implementation drops in behind this interface without touching
 * the shipping/snapshot/restore logic.
 */
public interface ObjectStore {

    void put(String key, Path file);

    void putBytes(String key, byte[] content);

    InputStream get(String key) throws IOException;

    /** Keys under the prefix, lexicographically sorted. */
    List<String> list(String prefix);

    boolean exists(String key);

    long size(String key);

    static ObjectStore open(String uri) {
        if (uri == null || uri.isBlank()) throw new ConfigException("object store URI required");
        String u = uri;
        if (u.startsWith("file://")) u = u.substring("file://".length());
        else if (u.startsWith("file:")) u = u.substring("file:".length());
        if (u.contains("://")) {
            throw new ConfigException("object store '" + uri + "': v1 supports filesystem URIs"
                    + " (file:/path or a plain path — point it at a mounted bucket);"
                    + " native S3 lands behind this same interface");
        }
        return new LocalFsObjectStore(Path.of(u));
    }
}
