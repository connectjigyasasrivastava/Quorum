package com.jigyasa.quorum;

/**
 * Holds the persistent and volatile election state for a single node.
 *
 * Raft state tracked here:
 *   - currentTerm: the latest term this node has seen
 *   - role:        leader, follower, or candidate
 *   - votedFor:    the candidate this node voted for in the current term
 *                  (null if it has not voted yet)
 *
 * All access is synchronized so the election and replication threads see
 * a consistent view.
 */
public class NodeState {

    private long currentTerm;
    private Role role;
    private NodeId votedFor;

    public NodeState() {
        this.currentTerm = 0;
        this.role = Role.FOLLOWER;
        this.votedFor = null;
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized Role role() {
        return role;
    }

    public synchronized NodeId votedFor() {
        return votedFor;
    }

    /**
     * Moves to a new, higher term. Resets the vote and steps down to follower.
     * Does nothing if the given term is not greater than the current one.
     */
    public synchronized void advanceTerm(long newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            role = Role.FOLLOWER;
            votedFor = null;
        }
    }

    /**
     * Becomes a candidate: increments the term and votes for self.
     * Returns the new term.
     */
    public synchronized long becomeCandidate(NodeId self) {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = self;
        return currentTerm;
    }

    public synchronized void becomeLeader() {
        role = Role.LEADER;
    }

    public synchronized void becomeFollower() {
        role = Role.FOLLOWER;
    }

    /**
     * Grants a vote to a candidate for the current term if not already used.
     * Returns true if the vote was granted.
     */
    public synchronized boolean grantVoteIfPossible(NodeId candidate) {
        if (votedFor == null || votedFor.equals(candidate)) {
            votedFor = candidate;
            return true;
        }
        return false;
    }
}