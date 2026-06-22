package com.jigyasa.quorum;

/**
 * A peer's reply to a VoteRequest.
 *
 *   - term:        the voter's current term (lets a stale candidate step down)
 *   - voteGranted: true if the vote was given to the candidate
 */
public record VoteResponse(long term, boolean voteGranted) {
}