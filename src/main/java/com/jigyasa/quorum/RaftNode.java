package com.jigyasa.quorum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RaftNode {

    private final ClusterConfig config;
    private final NodeState state;
    private final ReplicatedLog log;
    private final ElectionManager elections;
    private final ReplicationManager replication;
    private final CommitTracker commitTracker;
    private final RaftTransport transport;

    private final Map<NodeId, Long> nextIndex = new HashMap<>();

    private static final long MIN_TIMEOUT_MS = 150;
    private static final long MAX_TIMEOUT_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 50;

    private final AtomicLong lastContact = new AtomicLong(System.currentTimeMillis());
    private volatile long currentTimeoutMs = randomTimeout();
    private volatile boolean stopped = false;
    private volatile long lastHeartbeat = 0;

    private Thread ticker;

    private final ExecutorService rpcPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "raft-rpc");
        t.setDaemon(true);
        return t;
    });

    public RaftNode(ClusterConfig config, ReplicatedLog log, RaftTransport transport) {
        this.config = config;
        this.log = log;
        this.transport = transport;
        this.state = new NodeState();
        this.elections = new ElectionManager(state, log, config);
        this.replication = new ReplicationManager(state, log);
        this.commitTracker = new CommitTracker(config);
    }

    public void start() {
        stopped = false;
        recordContact();
        ticker = new Thread(this::runLoop, "raft-ticker-" + config.self());
        ticker.setDaemon(true);
        ticker.start();
    }

    public void stop() {
        stopped = true;
        if (ticker != null) {
            ticker.interrupt();
        }
        try {
            if (ticker != null) {
                ticker.join(200);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        rpcPool.shutdownNow();
    }

    private void runLoop() {
        while (!stopped) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (state.role() == Role.LEADER) {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
                    lastHeartbeat = now;
                    replicateToFollowers();
                }
                continue;
            }

            if (electionTimedOut()) {
                startElection();
            }
        }
    }

    private void startElection() {
        long term = state.becomeCandidate(config.self());
        recordContact();

        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();
        VoteRequest request = new VoteRequest(term, config.self(), lastLogIndex, lastLogTerm);

        AtomicInteger votes = new AtomicInteger(1);
        List<NodeId> peers = config.peers();

        for (NodeId peer : peers) {
            if (stopped || rpcPool.isShutdown()) {
                break;
            }
            try {
                rpcPool.submit(() -> {
                    VoteResponse resp = transport.sendVote(peer, request);
                    if (resp == null) {
                        return;
                    }
                    if (resp.term() > state.currentTerm()) {
                        state.advanceTerm(resp.term());
                        return;
                    }
                    if (resp.voteGranted()) {
                        int total = votes.incrementAndGet();
                        if (elections.hasWon(total)
                                && state.role() == Role.CANDIDATE
                                && state.currentTerm() == term) {
                            becomeLeader();
                        }
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                break;
            }
        }
    }

    private void becomeLeader() {
        state.becomeLeader();
        long nextIdx = log.lastIndex() + 1;
        synchronized (nextIndex) {
            nextIndex.clear();
            for (NodeId peer : config.peers()) {
                nextIndex.put(peer, nextIdx);
            }
        }
        lastHeartbeat = 0;
    }

    // Leader path: append a client write locally, to be replicated next tick.
    // Returns the index it was stored at, or -1 if this node is not leader.
    public long appendClientEntry(String topic, byte[] payload) {
        if (state.role() != Role.LEADER) {
            return -1;
        }
        return log.appendOnLeader(state.currentTerm(), topic, payload);
    }

    private void replicateToFollowers() {
        if (stopped) {
            return;
        }
        long term = state.currentTerm();
        for (NodeId peer : config.peers()) {
            if (stopped || rpcPool.isShutdown()) {
                return;
            }
            try {
                rpcPool.submit(() -> replicateToPeer(peer, term));
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                return; // pool shutting down during teardown; safe to stop
            }
        }
    }

    private void replicateToPeer(NodeId peer, long term) {
        long peerNext;
        synchronized (nextIndex) {
            peerNext = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        }

        long prevIndex = peerNext - 1;
        long prevTerm = prevIndex >= 0 ? entryTermAt(prevIndex) : 0;

        List<LogEntry> batch = new ArrayList<>();
        long lastIdx = log.lastIndex();
        for (long i = peerNext; i <= lastIdx; i++) {
            LogEntry e = log.get(i);
            if (e != null) {
                batch.add(e);
            }
        }

        ReplicationRequest request = new ReplicationRequest(
                term, config.self(), prevIndex, prevTerm, batch, commitTracker.commitIndex());

        ReplicationResponse resp = transport.sendAppend(peer, request);
        if (resp == null) {
            return;
        }

        if (resp.term() > state.currentTerm()) {
            state.advanceTerm(resp.term());
            return;
        }

        if (state.role() != Role.LEADER || state.currentTerm() != term) {
            return;
        }

        if (resp.success()) {
            synchronized (nextIndex) {
                nextIndex.put(peer, resp.matchIndex() + 1);
            }
            commitTracker.updateMatchIndex(peer, resp.matchIndex());
            commitTracker.recomputeCommitIndex(log.lastIndex());
        } else {
            synchronized (nextIndex) {
                long current = nextIndex.getOrDefault(peer, peerNext);
                nextIndex.put(peer, Math.max(0, current - 1));
            }
        }
    }

    private long entryTermAt(long index) {
        LogEntry e = log.get(index);
        return e == null ? 0 : e.term();
    }

    public VoteResponse onVoteRequest(VoteRequest request) {
        VoteResponse response = elections.handleVoteRequest(request);
        if (response.voteGranted()) {
            recordContact();
        }
        return response;
    }

    public ReplicationResponse onAppendRequest(ReplicationRequest request) {
        if (request.term() >= state.currentTerm()) {
            recordContact();
        }
        return replication.handle(request);
    }

    private static long randomTimeout() {
        return ThreadLocalRandom.current().nextLong(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS + 1);
    }

    private void recordContact() {
        lastContact.set(System.currentTimeMillis());
        currentTimeoutMs = randomTimeout();
    }

    private boolean electionTimedOut() {
        long silence = System.currentTimeMillis() - lastContact.get();
        return silence >= currentTimeoutMs;
    }

    public NodeId id() {
        return config.self();
    }

    public Role role() {
        return state.role();
    }

    public long currentTerm() {
        return state.currentTerm();
    }

    public long commitIndex() {
        return commitTracker.commitIndex();
    }

    public ReplicatedLog log() {
        return log;
    }
}