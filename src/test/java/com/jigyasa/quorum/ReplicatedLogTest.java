package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicatedLogTest {

    @Test
    void leaderAppendAssignsIncreasingIndexes() {
        ReplicatedLog log = new ReplicatedLog();
        assertEquals(0, log.appendOnLeader(1, "t", "a".getBytes()));
        assertEquals(1, log.appendOnLeader(1, "t", "b".getBytes()));
        assertEquals(1, log.lastIndex());
    }

    @Test
    void followerAcceptsEntriesOnEmptyLog() {
        ReplicatedLog log = new ReplicatedLog();
        List<LogEntry> entries = List.of(
                new LogEntry(1, 0, "t", "a".getBytes()),
                new LogEntry(1, 1, "t", "b".getBytes()));

        boolean ok = log.appendFromLeader(-1, 0, entries);

        assertTrue(ok);
        assertEquals(2, log.size());
    }

    @Test
    void followerRejectsWhenPrevIndexMissing() {
        ReplicatedLog log = new ReplicatedLog();
        // prevIndex 5 does not exist on an empty log
        boolean ok = log.appendFromLeader(5, 1,
                List.of(new LogEntry(1, 6, "t", "x".getBytes())));
        assertFalse(ok);
    }

    @Test
    void followerRejectsOnTermMismatch() {
        ReplicatedLog log = new ReplicatedLog();
        log.appendFromLeader(-1, 0,
                List.of(new LogEntry(1, 0, "t", "a".getBytes())));

        // claim prevTerm 2 at index 0, but it is actually term 1
        boolean ok = log.appendFromLeader(0, 2,
                List.of(new LogEntry(2, 1, "t", "b".getBytes())));
        assertFalse(ok);
    }

    @Test
    void conflictingTailIsTruncatedAndReplaced() {
        ReplicatedLog log = new ReplicatedLog();
        log.appendFromLeader(-1, 0, List.of(
                new LogEntry(1, 0, "t", "a".getBytes()),
                new LogEntry(1, 1, "t", "old".getBytes())));

        // new leader (term 2) overwrites index 1
        boolean ok = log.appendFromLeader(0, 1, List.of(
                new LogEntry(2, 1, "t", "new".getBytes())));

        assertTrue(ok);
        assertEquals(2, log.size());
        assertEquals("new", new String(log.get(1).payload()));
        assertEquals(2, log.get(1).term());
    }
}