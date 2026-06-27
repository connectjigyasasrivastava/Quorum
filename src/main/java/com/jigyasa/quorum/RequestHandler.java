package com.jigyasa.quorum;

import java.io.IOException;
import java.util.List;
import java.util.Base64;

public class RequestHandler {

    private final PersistentMessageQueue queue;
    private final RaftNode raft;

    public RequestHandler(PersistentMessageQueue queue) {
        this(queue, null);
    }

    public RequestHandler(PersistentMessageQueue queue, RaftNode raft) {
        this.queue = queue;
        this.raft = raft;
    }

    public String handle(String line) {
        if (line == null || line.isBlank()) {
            return Protocol.ERROR + " empty request";
        }

        String[] parts = line.trim().split(" ");
        String command = parts[0];

        try {
            switch (command) {
                case Protocol.CREATE -> {
                    requireArgs(parts, 2);
                    queue.createTopic(parts[1]);
                    return Protocol.OK + " created";
                }
                case Protocol.PRODUCE -> {
                    requireArgs(parts, 3);
                    byte[] payload = Base64.getDecoder().decode(parts[2]);
                    long offset = queue.produce(parts[1], payload);
                    return Protocol.OK + " " + offset;
                }
                case Protocol.CONSUME -> {
                    requireArgs(parts, 3);
                    long offset = Long.parseLong(parts[2]);
                    Message message = queue.consume(parts[1], offset);
                    if (message == null) {
                        return Protocol.EMPTY;
                    }
                    String encoded = Base64.getEncoder().encodeToString(message.payload());
                    return Protocol.OK + " " + encoded;
                }
                case Protocol.SIZE -> {
                    requireArgs(parts, 2);
                    return Protocol.OK + " " + queue.size(parts[1]);
                }
                case Protocol.VOTE -> {
                    return handleVote(parts);
                }
                case Protocol.APPEND -> {
                    return handleAppend(parts);
                }
                default -> {
                    return Protocol.ERROR + " unknown command: " + command;
                }
            }
        } catch (IllegalArgumentException e) {
            return Protocol.ERROR + " " + e.getMessage();
        } catch (IOException e) {
            return Protocol.ERROR + " io failure: " + e.getMessage();
        }
    }

    // VOTE <term> <candidateHost:port> <lastLogIndex> <lastLogTerm>
    private String handleVote(String[] parts) {
        if (raft == null) {
            return Protocol.ERROR + " raft not enabled";
        }
        requireArgs(parts, 5);
        long term = Long.parseLong(parts[1]);
        NodeId candidate = NodeId.parse(parts[2]);
        long lastLogIndex = Long.parseLong(parts[3]);
        long lastLogTerm = Long.parseLong(parts[4]);

        VoteRequest request = new VoteRequest(term, candidate, lastLogIndex, lastLogTerm);
        VoteResponse response = raft.onVoteRequest(request);

        return Protocol.VOTERESULT + " " + response.term() + " " + response.voteGranted();
    }

    // APPEND <term> <leaderHost:port> <prevIndex> <prevTerm> <leaderCommit> <entryCount> [<entry> ...]
    private String handleAppend(String[] parts) {
        if (raft == null) {
            return Protocol.ERROR + " raft not enabled";
        }
        requireArgs(parts, 7);
        long term = Long.parseLong(parts[1]);
        NodeId leader = NodeId.parse(parts[2]);
        long prevIndex = Long.parseLong(parts[3]);
        long prevTerm = Long.parseLong(parts[4]);
        long leaderCommit = Long.parseLong(parts[5]);
        int entryCount = Integer.parseInt(parts[6]);

        if (parts.length < 7 + entryCount) {
            throw new IllegalArgumentException("expected " + entryCount + " entries");
        }
        List<LogEntry> entries = RaftTransport.decodeEntries(parts, 7, entryCount);

        ReplicationRequest request = new ReplicationRequest(
                term, leader, prevIndex, prevTerm, entries, leaderCommit);
        ReplicationResponse response = raft.onAppendRequest(request);

        return Protocol.APPENDRESULT + " " + response.term() + " "
                + response.success() + " " + response.matchIndex();
    }

    private void requireArgs(String[] parts, int expected) {
        if (parts.length < expected) {
            throw new IllegalArgumentException("expected " + expected + " arguments");
        }
    }
}