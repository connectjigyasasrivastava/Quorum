package com.jigyasa.quorum;

/**
 * A single entry in the replicated log.
 *
 * Each entry carries:
 *   - term:    the leadership term in which it was created (used by elections)
 *   - index:   its position in the log (0-based, monotonically increasing)
 *   - topic:   the topic the underlying message belongs to
 *   - payload: the raw message bytes
 *
 * Entries are immutable once created.
 */
public record LogEntry(long term, long index, String topic, byte[] payload) {
}