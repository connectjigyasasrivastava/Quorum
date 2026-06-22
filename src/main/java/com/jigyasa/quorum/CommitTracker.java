package com.jigyasa.quorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks how far each follower has replicated the leader's log, and from
 * that computes the commit index: the highest log index that a quorum of
 * nodes has stored.
 *
 * An entry is "committed" once it is safely on a majority of nodes, after
 * which it can be applied to the state machine and acknowledged to clients.
 */
public class CommitTracker {

    private final ClusterConfig config;
    private final Map<NodeId, Long> matchIndex = new HashMap<>();
    private long commitIndex = -1;

    public CommitTracker(ClusterConfig config) {
        this.config = config;
    }

    /**
     * Records that a follower has replicated up to the given index.
     */
    public synchronized void updateMatchIndex(NodeId follower, long index) {
        matchIndex.merge(follower, index, Math::max);
    }

    /**
     * Recomputes the commit index given the leader's own last index.
     *
     * The leader counts as having every entry up to leaderLastIndex.
     * The commit index is the highest index stored on at least a quorum
     * of nodes.
     */
    public synchronized long recomputeCommitIndex(long leaderLastIndex) {
        List<Long> indices = new ArrayList<>();
        indices.add(leaderLastIndex);              // the leader itself
        indices.addAll(matchIndex.values());       // each follower

        // Sort descending; the entry at position (quorum - 1) is replicated
        // on at least 'quorum' nodes.
        indices.sort(Collections.reverseOrder());
        int quorumIndex = config.quorum() - 1;

        if (quorumIndex < indices.size()) {
            long candidate = indices.get(quorumIndex);
            if (candidate > commitIndex) {
                commitIndex = candidate;
            }
        }
        return commitIndex;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }
}