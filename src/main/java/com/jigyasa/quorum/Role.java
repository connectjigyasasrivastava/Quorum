package com.jigyasa.quorum;

/**
 * The role a node currently plays in the cluster.
 *
 * LEADER   - accepts writes and replicates them to followers
 * FOLLOWER - receives replicated entries from the leader
 * CANDIDATE - is currently trying to become leader during an election
 */
public enum Role {
    LEADER,
    FOLLOWER,
    CANDIDATE
}