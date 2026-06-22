package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationManagerTest {

    private NodeId leader() {
        return new NodeId("localhost", 9092);
    }

    @Test
    void acceptsEntriesFromValidLeader() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        ReplicationRequest request = new ReplicationRequest(
                1, leader(), -1, 0,
                List.of(new LogEntry(1, 0, "t", "a".getBytes())),
                -1);

        ReplicationResponse response = manager.handle(request);

        assertTrue(response.success());
        assertEquals(0, response.matchIndex());
        assertEquals(1, log.size());
    }

    @Test
    void rejectsStaleLeader() {
        NodeState state = new NodeState();
        state.advanceTerm(5);
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        ReplicationRequest request = new ReplicationRequest(
                3, leader(), -1, 0,
                List.of(new LogEntry(3, 0, "t", "x".getBytes())),
                -1);

        ReplicationResponse response = manager.handle(request);

        assertFalse(response.success());
        assertEquals(5, response.term());
    }

    @Test
    void adoptsHigherTermFromLeader() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        manager.handle(new ReplicationRequest(
                7, leader(), -1, 0,
                List.of(new LogEntry(7, 0, "t", "a".getBytes())),
                -1));

        assertEquals(7, state.currentTerm());
        assertEquals(Role.FOLLOWER, state.role());
    }

    @Test
    void heartbeatWithNoEntriesSucceeds() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        ReplicationResponse response = manager.handle(new ReplicationRequest(
                1, leader(), -1, 0, List.of(), -1));

        assertTrue(response.success());
        assertEquals(-1, response.matchIndex());
    }

    @Test
    void advancesCommitIndexUpToWhatItHas() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        manager.handle(new ReplicationRequest(
                1, leader(), -1, 0,
                List.of(new LogEntry(1, 0, "t", "a".getBytes()),
                        new LogEntry(1, 1, "t", "b".getBytes())),
                1));

        assertEquals(1, manager.commitIndex());
    }

    @Test
    void rejectsWhenLogIsInconsistent() {
        NodeState state = new NodeState();
        ReplicatedLog log = new ReplicatedLog();
        ReplicationManager manager = new ReplicationManager(state, log);

        // prevIndex 3 does not exist on an empty log
        ReplicationResponse response = manager.handle(new ReplicationRequest(
                1, leader(), 3, 1,
                List.of(new LogEntry(1, 4, "t", "x".getBytes())),
                -1));

        assertFalse(response.success());
    }
}