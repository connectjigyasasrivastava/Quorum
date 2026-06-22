package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeStateTest {

    @Test
    void startsAsFollowerAtTermZero() {
        NodeState state = new NodeState();
        assertEquals(0, state.currentTerm());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @Test
    void becomingCandidateIncrementsTermAndVotesForSelf() {
        NodeState state = new NodeState();
        NodeId self = new NodeId("localhost", 9092);

        long term = state.becomeCandidate(self);

        assertEquals(1, term);
        assertEquals(Role.CANDIDATE, state.role());
        assertEquals(self, state.votedFor());
    }

    @Test
    void advancingTermStepsDownToFollowerAndResetsVote() {
        NodeState state = new NodeState();
        NodeId self = new NodeId("localhost", 9092);
        state.becomeCandidate(self);
        state.becomeLeader();

        state.advanceTerm(5);

        assertEquals(5, state.currentTerm());
        assertEquals(Role.FOLLOWER, state.role());
        assertEquals(null, state.votedFor());
    }

    @Test
    void grantsVoteOnlyOncePerTerm() {
        NodeState state = new NodeState();
        NodeId a = new NodeId("localhost", 9093);
        NodeId b = new NodeId("localhost", 9094);

        assertTrue(state.grantVoteIfPossible(a));
        assertTrue(state.grantVoteIfPossible(a));
        assertFalse(state.grantVoteIfPossible(b));
    }

    @Test
    void advanceTermIgnoresLowerTerm() {
        NodeState state = new NodeState();
        state.advanceTerm(3);
        state.advanceTerm(2);
        assertEquals(3, state.currentTerm());
    }
}