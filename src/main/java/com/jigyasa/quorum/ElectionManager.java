package com.jigyasa.quorum;

/**
 * Decides how a node responds to vote requests and tallies election results.
 *
 * This class holds the pure election rules (no networking), so the logic
 * can be tested directly:
 *   - a node grants a vote only if the candidate's term is at least its own,
 *     it has not already voted for someone else this term, and the
 *     candidate's log is at least as up-to-date as its own
 *   - a candidate wins once it collects a quorum of votes
 */
public class ElectionManager {

    private final NodeState state;
    private final ReplicatedLog log;
    private final ClusterConfig config;

    public ElectionManager(NodeState state, ReplicatedLog log, ClusterConfig config) {
        this.state = state;
        this.log = log;
        this.config = config;
    }

    /**
     * Handles an incoming vote request and returns the response.
     */
    public synchronized VoteResponse handleVoteRequest(VoteRequest request) {
        // If the candidate's term is newer, step up to it first.
        if (request.term() > state.currentTerm()) {
            state.advanceTerm(request.term());
        }

        // Reject stale candidates outright.
        if (request.term() < state.currentTerm()) {
            return new VoteResponse(state.currentTerm(), false);
        }

        boolean logOk = candidateLogIsUpToDate(
                request.lastLogIndex(), request.lastLogTerm());

        if (logOk && state.grantVoteIfPossible(request.candidateId())) {
            return new VoteResponse(state.currentTerm(), true);
        }

        return new VoteResponse(state.currentTerm(), false);
    }

    /**
     * Raft's up-to-date rule: a candidate's log is at least as current as ours
     * if its last term is higher, or the terms tie and its index is >= ours.
     */
    private boolean candidateLogIsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long ourLastTerm = log.lastTerm();
        long ourLastIndex = log.lastIndex();

        if (candidateLastTerm != ourLastTerm) {
            return candidateLastTerm > ourLastTerm;
        }
        return candidateLastIndex >= ourLastIndex;
    }

    /**
     * Returns true if the given number of votes is a quorum (majority).
     */
    public boolean hasWon(int votes) {
        return votes >= config.quorum();
    }
}