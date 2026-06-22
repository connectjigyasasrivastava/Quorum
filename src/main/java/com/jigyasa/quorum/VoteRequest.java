package com.jigyasa.quorum;

/**
 * Sent by a candidate to peers asking for their vote during an election.
 * Mirrors Raft's RequestVote RPC.
 *
 *   - term:         the candidate's term
 *   - candidateId:  who is asking for the vote
 *   - lastLogIndex: index of the candidate's last log entry
 *   - lastLogTerm:  term of the candidate's last log entry
 *
 * lastLogIndex/lastLogTerm let voters reject candidates whose logs are
 * less up-to-date than their own (the election restriction).
 */
public record VoteRequest(
        long term,
        NodeId candidateId,
        long lastLogIndex,
        long lastLogTerm) {
}