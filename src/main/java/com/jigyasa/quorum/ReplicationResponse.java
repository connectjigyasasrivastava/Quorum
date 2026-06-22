package com.jigyasa.quorum;

/**
 * A follower's reply to a ReplicationRequest.
 *
 *   - term:       the follower's current term (lets a stale leader step down)
 *   - success:    true if the entries were accepted
 *   - matchIndex: the highest log index the follower now has, when successful
 */
public record ReplicationResponse(long term, boolean success, long matchIndex) {
}