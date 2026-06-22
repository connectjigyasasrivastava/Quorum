package com.jigyasa.quorum;

import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory replicated log of entries, shared in structure by leaders
 * and followers.
 *
 * Provides the safety checks Raft needs:
 *   - appendOnLeader: leader adds a new entry at the next index
 *   - appendFromLeader: follower accepts entries only if they line up with
 *     its existing log (matching prevIndex/prevTerm), deleting conflicts
 *
 * Thread-safe via coarse synchronization.
 */
public class ReplicatedLog {

    private final List<LogEntry> entries = new ArrayList<>();

    /**
     * Leader path: append a new entry for the given term and return its index.
     */
    public synchronized long appendOnLeader(long term, String topic, byte[] payload) {
        long index = entries.size();
        entries.add(new LogEntry(term, index, topic, payload));
        return index;
    }

    /**
     * Follower path: try to append entries from the leader.
     *
     * Returns true if the log is consistent and the entries were applied.
     * Returns false if the consistency check at prevIndex/prevTerm fails,
     * which tells the leader to retry with earlier entries.
     */
    public synchronized boolean appendFromLeader(long prevIndex, long prevTerm,
                                                 List<LogEntry> newEntries) {
        // Consistency check: the entry at prevIndex must exist and match prevTerm.
        if (prevIndex >= 0) {
            if (prevIndex >= entries.size()) {
                return false; // we are missing entries before these
            }
            if (entries.get((int) prevIndex).term() != prevTerm) {
                return false; // term mismatch -> conflict
            }
        }

        // Append, overwriting any conflicting tail.
        long insertAt = prevIndex + 1;
        for (int i = 0; i < newEntries.size(); i++) {
            long targetIndex = insertAt + i;
            LogEntry incoming = newEntries.get(i);

            if (targetIndex < entries.size()) {
                // Existing entry at this index; if it conflicts, truncate.
                if (entries.get((int) targetIndex).term() != incoming.term()) {
                    truncateFrom((int) targetIndex);
                    entries.add(incoming);
                }
                // else: same entry already present, skip
            } else {
                entries.add(incoming);
            }
        }
        return true;
    }

    private void truncateFrom(int index) {
        while (entries.size() > index) {
            entries.remove(entries.size() - 1);
        }
    }

    public synchronized LogEntry get(long index) {
        if (index < 0 || index >= entries.size()) {
            return null;
        }
        return entries.get((int) index);
    }

    public synchronized long lastIndex() {
        return entries.size() - 1;
    }

    public synchronized long lastTerm() {
        if (entries.isEmpty()) {
            return 0;
        }
        return entries.get(entries.size() - 1).term();
    }

    public synchronized int size() {
        return entries.size();
    }
}