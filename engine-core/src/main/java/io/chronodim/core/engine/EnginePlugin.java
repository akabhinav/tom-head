package io.chronodim.core.engine;

/**
 * Extension point discovered via {@link java.util.ServiceLoader}: the export
 * publisher and durability shipper attach here without engine-core depending on
 * their modules. Callbacks arrive after the commit is WAL-durable.
 */
public interface EnginePlugin extends AutoCloseable {

    /** Called once after recovery completes; return false to stay detached (e.g. feature disabled). */
    boolean attach(EngineImpl engine);

    /** A transaction became durable. Called outside engine locks; must not block long. */
    default void onCommit(long txnId) {}

    /**
     * WAL prune veto: may the segment (whose highest txn is {@code maxTxnInSegment})
     * be deleted locally? The publisher vetoes segments it still needs to tail;
     * the shipper vetoes segments not yet fully uploaded.
     */
    default boolean allowWalPrune(String segmentName, long maxTxnInSegment) {
        return true;
    }

    default java.util.Map<String, Object> stats() {
        return java.util.Map.of();
    }

    @Override
    default void close() {}
}
