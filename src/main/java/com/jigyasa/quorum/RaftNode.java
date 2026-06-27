package com.jigyasa.quorum;

import java.util.HashMap;
import java.util.Map;

/**
 * The orchestrator for one node's participation in the Raft cluster.
 *
 * It owns the election/replication state and the managers that hold the
 * pure rules, and (in later steps) runs the election timer and the leader
 * heartbeat/replication loop.
 *
 * Built up in stages:
 *   Stage 1 (this): state, managers, and the two RPC handlers (vote, append)
 *   Stage 2: election timer + candidate round
 *   Stage 3: leader heartbeat + log replication + commit advancement
 */
public class RaftNode {

    private final ClusterConfig config;
    private final NodeState state;
    private final ReplicatedLog log;
    private final ElectionManager elections;
    private final ReplicationManager replication;
    private final CommitTracker commitTracker;
    private final RaftTransport transport;

    // Leader-only progress tracking: next index to send each peer.
    private final Map<NodeId, Long> nextIndex = new HashMap<>();

    public RaftNode(ClusterConfig config, ReplicatedLog log, RaftTransport transport) {
        this.config = config;
        this.log = log;
        this.transport = transport;
        this.state = new NodeState();
        this.elections = new ElectionManager(state, log, config);
        this.replication = new ReplicationManager(state, log);
        this.commitTracker = new CommitTracker(config);
    }

    // ---------- RPC handlers (called by RequestHandler for incoming lines) ----------

    /**
     * Handles an incoming vote request from a candidate.
     */
    public VoteResponse onVoteRequest(VoteRequest request) {
        return elections.handleVoteRequest(request);
    }

    /**
     * Handles an incoming replication (AppendEntries) request from a leader.
     */
    public ReplicationResponse onAppendRequest(ReplicationRequest request) {
        return replication.handle(request);
    }

    // ---------- Accessors (useful for tests and the server) ----------

    public NodeId id() {
        return config.self();
    }

    public Role role() {
        return state.role();
    }

    public long currentTerm() {
        return state.currentTerm();
    }

    public long commitIndex() {
        return commitTracker.commitIndex();
    }

    public ReplicatedLog log() {
        return log;
    }
}