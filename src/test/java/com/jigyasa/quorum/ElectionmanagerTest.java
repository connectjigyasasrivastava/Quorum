package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElectionManagerTest {

    private ElectionManager newManager(NodeState state, ReplicatedLog log) {
        ClusterConfig config = new ClusterConfig(
                new NodeId("localhost", 9092),
                List.of(new NodeId("localhost", 9093),
                        new NodeId("localhost", 9094)));
        return new ElectionManager(state, log, config);
    }

    @Test
    void grantsVoteToValidCandidate() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ElectionManager manager = newManager(state, log);

        VoteRequest request = new VoteRequest(
                1, new NodeId("localhost", 9093), -1, 0);

        VoteResponse response = manager.handleVoteRequest(request);

        assertTrue(response.voteGranted());
        assertEquals(1, response.term());
    }

    @Test
    void rejectsCandidateWithStaleTerm() {
        NodeState state = new NodeState();
        state.advanceTerm(5);
        ReplicatedLog log = new ReplicatedLog();
        ElectionManager manager = newManager(state, log);

        VoteRequest request = new VoteRequest(
                3, new NodeId("localhost", 9093), -1, 0);

        VoteResponse response = manager.handleVoteRequest(request);

        assertFalse(response.voteGranted());
        assertEquals(5, response.term());
    }

    @Test
    void doesNotVoteTwiceInSameTerm() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ElectionManager manager = newManager(state, log);

        manager.handleVoteRequest(
                new VoteRequest(1, new NodeId("localhost", 9093), -1, 0));
        VoteResponse second = manager.handleVoteRequest(
                new VoteRequest(1, new NodeId("localhost", 9094), -1, 0));

        assertFalse(second.voteGranted());
    }

    @Test
    void rejectsCandidateWithLessUpToDateLog() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        // our log has an entry at term 2
        log.appendFromLeader(-1, 0,
                List.of(new LogEntry(2, 0, "t", "a".getBytes())));
        ElectionManager manager = newManager(state, log);

        // candidate's last log term is 1 (older) -> should be rejected
        VoteRequest request = new VoteRequest(
                3, new NodeId("localhost", 9093), 0, 1);

        VoteResponse response = manager.handleVoteRequest(request);

        assertFalse(response.voteGranted());
    }

    @Test
    void quorumIsRequiredToWin() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ElectionManager manager = newManager(state, log);

        assertFalse(manager.hasWon(1));
        assertTrue(manager.hasWon(2)); // quorum of a 3-node cluster
    }
}