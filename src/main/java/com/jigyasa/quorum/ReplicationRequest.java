package com.jigyasa.quorum;

import java.util.List;

/**
 * Sent by the leader to a follower to replicate log entries.
 *
 * Mirrors the core of Raft's AppendEntries RPC:
 *   - term:         the leader's current term
 *   - leaderId:     who is sending this
 *   - prevIndex:    index of the entry immediately preceding the new ones
 *   - prevTerm:     term of that preceding entry (consistency check)
 *   - entries:      the new entries to append (empty list = heartbeat)
 *   - leaderCommit: the highest index the leader has committed
 */
public record ReplicationRequest(
        long term,
        NodeId leaderId,
        long prevIndex,
        long prevTerm,
        List<LogEntry> entries,
        long leaderCommit) {
}