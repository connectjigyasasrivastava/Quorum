package com.jigyasa.quorum;

/**
 * Handles the follower side of replication: it receives a leader's
 * ReplicationRequest, runs the safety checks, applies the entries to the
 * local log, and updates term and commit information.
 *
 * This is the pure logic (no networking) so it can be tested directly.
 */
public class ReplicationManager {

    private final NodeState state;
    private final ReplicatedLog log;
    private volatile long commitIndex = -1;

    public ReplicationManager(NodeState state, ReplicatedLog log) {
        this.state = state;
        this.log = log;
    }

    /**
     * Processes a replication request from a leader and returns the response.
     */
    public synchronized ReplicationResponse handle(ReplicationRequest request) {
        // Reject a stale leader.
        if (request.term() < state.currentTerm()) {
            return new ReplicationResponse(state.currentTerm(), false, log.lastIndex());
        }

        // A valid leader at an equal or higher term: adopt its term and
        // step down to follower (we recognize its authority).
        if (request.term() > state.currentTerm()) {
            state.advanceTerm(request.term());
        }
        state.becomeFollower();

        // Run the log consistency check and append.
        boolean ok = log.appendFromLeader(
                request.prevIndex(), request.prevTerm(), request.entries());

        if (!ok) {
            return new ReplicationResponse(state.currentTerm(), false, log.lastIndex());
        }

        // Advance our commit index, but never past what we actually have.
        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), log.lastIndex());
        }

        return new ReplicationResponse(state.currentTerm(), true, log.lastIndex());
    }

    public long commitIndex() {
        return commitIndex;
    }
}