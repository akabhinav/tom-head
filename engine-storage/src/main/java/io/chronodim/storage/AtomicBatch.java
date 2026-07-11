package io.chronodim.storage;

import java.util.ArrayList;
import java.util.List;

/** An ordered set of mutations applied atomically. Reusable via {@link #clear()}. */
public final class AtomicBatch {

    public sealed interface Mutation permits Put, Delete {
        byte[] key();
    }

    public record Put(byte[] key, byte[] value) implements Mutation {}

    public record Delete(byte[] key) implements Mutation {}

    private final List<Mutation> mutations = new ArrayList<>();

    public AtomicBatch put(byte[] key, byte[] value) {
        mutations.add(new Put(key, value));
        return this;
    }

    public AtomicBatch delete(byte[] key) {
        mutations.add(new Delete(key));
        return this;
    }

    public List<Mutation> mutations() {
        return mutations;
    }

    public int size() {
        return mutations.size();
    }

    public boolean isEmpty() {
        return mutations.isEmpty();
    }

    public void clear() {
        mutations.clear();
    }
}
