package com.jigyasa.quorum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The orchestrator for one node's participation in the Raft cluster.
 *
 * It owns the election/replication state and the managers that hold the
 * pure rules, and (in later steps) runs the election timer and the leader
 * heartbeat/replication loop.
 *
 * Built up in stages:
 *   Stage 1: state, managers, and the two RPC handlers (vote, append)
 *   Stage 2 (this): election timer that detects leader silence
 *   Stage 3: candidate election round
 *   Stage 4: leader heartbeat + log replication + commit advancement
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

    // Election timing
    // If no valid leader contact arrives within a randomized timeout, the
    // node starts an election. Randomization spreads out timeouts so nodes
    // don't all become candidates at the same instant (split votes).
    private static final long MIN_TIMEOUT_MS = 150;
    private static final long MAX_TIMEOUT_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 50;

    // Timestamp (ms) of the last time we heard from a valid leader or
    // granted a vote. The election timer measures silence from here.
    private final AtomicLong lastContact = new AtomicLong(System.currentTimeMillis());

    private volatile long currentTimeoutMs = randomTimeout();
    private volatile boolean stopped = false;

    public RaftNode(ClusterConfig config, ReplicatedLog log, RaftTransport transport) {
        this.config = config;
        this.log = log;
        this.transport = transport;
        this.state = new NodeState();
        this.elections = new ElectionManager(state, log, config);
        this.replication = new ReplicationManager(state, log);
        this.commitTracker = new CommitTracker(config);
    }

    // RPC handlers (called by RequestHandler for incoming lines)

    /**
     * Handles an incoming vote request from a candidate.
     * Granting a vote counts as valid contact, so we reset the timer to
     * avoid immediately challenging a candidate we just supported.
     */
    public VoteResponse onVoteRequest(VoteRequest request) {
        VoteResponse response = elections.handleVoteRequest(request);
        if (response.voteGranted()) {
            recordContact();
        }
        return response;
    }

    /**
     * Handles an incoming replication (AppendEntries) request from a leader.
     * A request from a current/newer-term leader is valid contact and
     * resets the election timer (this is what keeps healthy followers calm).
     */
    public ReplicationResponse onAppendRequest(ReplicationRequest request) {
        if (request.term() >= state.currentTerm()) {
            recordContact();
        }
        return replication.handle(request);
    }

    // Election timing

    private static long randomTimeout() {
        return ThreadLocalRandom.current().nextLong(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS + 1);
    }

    /** Marks "we just heard from a valid leader/candidate" — resets the clock. */
    private void recordContact() {
        lastContact.set(System.currentTimeMillis());
        currentTimeoutMs = randomTimeout();
    }

    /** True if too long has passed since the last valid contact. */
    private boolean electionTimedOut() {
        long silence = System.currentTimeMillis() - lastContact.get();
        return silence >= currentTimeoutMs;
    }

    public void stop() {
        stopped = true;
    }

    //  Accessors (useful for tests and the server)

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