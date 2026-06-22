package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommitTrackerTest {

    private ClusterConfig threeNodeConfig() {
        return new ClusterConfig(
                new NodeId("localhost", 9092),
                List.of(new NodeId("localhost", 9093),
                        new NodeId("localhost", 9094)));
    }

    @Test
    void nothingCommittedInitially() {
        CommitTracker tracker = new CommitTracker(threeNodeConfig());
        assertEquals(-1, tracker.commitIndex());
    }

    @Test
    void commitsWhenQuorumHasEntry() {
        CommitTracker tracker = new CommitTracker(threeNodeConfig());
        NodeId follower1 = new NodeId("localhost", 9093);

        // leader has index 2, one follower has index 2 -> 2 of 3 nodes = quorum
        tracker.updateMatchIndex(follower1, 2);
        long commit = tracker.recomputeCommitIndex(2);

        assertEquals(2, commit);
    }

    @Test
    void doesNotCommitWithoutQuorum() {
        CommitTracker tracker = new CommitTracker(threeNodeConfig());

        // only the leader has index 5, no follower has caught up
        long commit = tracker.recomputeCommitIndex(5);

        assertEquals(-1, commit);
    }

    @Test
    void commitIndexAdvancesAsFollowersCatchUp() {
        CommitTracker tracker = new CommitTracker(threeNodeConfig());
        NodeId follower1 = new NodeId("localhost", 9093);
        NodeId follower2 = new NodeId("localhost", 9094);

        tracker.updateMatchIndex(follower1, 1);
        assertEquals(1, tracker.recomputeCommitIndex(3));

        tracker.updateMatchIndex(follower2, 3);
        assertEquals(3, tracker.recomputeCommitIndex(3));
    }

    @Test
    void commitIndexNeverGoesBackwards() {
        CommitTracker tracker = new CommitTracker(threeNodeConfig());
        NodeId follower1 = new NodeId("localhost", 9093);

        tracker.updateMatchIndex(follower1, 5);
        assertEquals(5, tracker.recomputeCommitIndex(5));

        // a later recompute with a lower leader index must not lower commit
        assertEquals(5, tracker.recomputeCommitIndex(2));
    }
}