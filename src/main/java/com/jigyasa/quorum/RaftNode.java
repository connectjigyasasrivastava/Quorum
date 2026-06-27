package com.jigyasa.quorum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The orchestrator for one node's participation in the Raft cluster.
 *
 * It owns the election/replication state and the managers that hold the
 * pure rules, runs the election timer, and runs the candidate election
 * round. The leader heartbeat/replication loop is added in the next stage.
 *
 * Built up in stages:
 *   Stage 1: state, managers, and the two RPC handlers (vote, append)
 *   Stage 2: election timer that detects leader silence
 *   Stage 3 (this): background loop + candidate election round
 *   Stage 4: leader heartbeat + log replication + commit advancement
 */
public class RaftNode {

    private final ClusterConfig config;
    private final NodeState state;
    private final ReplicatedLog log;
    private final ElectionManager elections;
    private final ReplicationManager replication;
    private final CommitTracker commitTracker;
    private final RaftTransport transport;

    // Leader-only progress tracking: next index to send each peer.
    private final Map<NodeId, Long> nextIndex = new HashMap<>();

    // --- Election timing ---
    private static final long MIN_TIMEOUT_MS = 150;
    private static final long MAX_TIMEOUT_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 50;

    private final AtomicLong lastContact = new AtomicLong(System.currentTimeMillis());
    private volatile long currentTimeoutMs = randomTimeout();
    private volatile boolean stopped = false;

    // Background thread that drives elections (and, later, heartbeats).
    private Thread ticker;
    // Pool for sending vote/append RPCs to peers in parallel.
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

    // ---------- Lifecycle ----------

    /** Starts the background election timer thread. */
    public void start() {
        stopped = false;
        recordContact(); // start the clock fresh
        ticker = new Thread(this::runLoop, "raft-ticker-" + config.self());
        ticker.setDaemon(true);
        ticker.start();
    }

    public void stop() {
        stopped = true;
        if (ticker != null) {
            ticker.interrupt();
        }
        rpcPool.shutdownNow();
    }

    /**
     * The heartbeat of the node. While running, it repeatedly checks whether
     * the election timer has expired and, if so and we are not leader, starts
     * an election.
     */
    private void runLoop() {
        while (!stopped) {
            try {
                Thread.sleep(20); // tick granularity
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (state.role() == Role.LEADER) {
                // Leader behavior (heartbeats/replication) added next stage.
                continue;
            }

            if (electionTimedOut()) {
                startElection();
            }
        }
    }

    // ---------- Candidate election round ----------

    /**
     * Runs one full election attempt:
     *   1. become candidate (new term, vote for self)
     *   2. ask every peer for a vote, in parallel
     *   3. if a quorum grants, become leader; if a higher term is seen,
     *      step down; otherwise the next timeout will retry.
     */
    private void startElection() {
        long term = state.becomeCandidate(config.self());
        recordContact(); // reset timer so a failed election retries after a fresh timeout

        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();
        VoteRequest request = new VoteRequest(term, config.self(), lastLogIndex, lastLogTerm);

        // We vote for ourselves; count starts at 1.
        AtomicInteger votes = new AtomicInteger(1);
        List<NodeId> peers = config.peers();

        for (NodeId peer : peers) {
            rpcPool.submit(() -> {
                VoteResponse resp = transport.sendVote(peer, request);
                if (resp == null) {
                    return; // peer unreachable; counts as no vote
                }
                // If a peer reports a higher term, we are stale: step down.
                if (resp.term() > state.currentTerm()) {
                    state.advanceTerm(resp.term());
                    return;
                }
                if (resp.voteGranted()) {
                    int total = votes.incrementAndGet();
                    // Promote as soon as a quorum is reached, but only if we
                    // are still a candidate in the same term (no newer leader
                    // has displaced us in the meantime).
                    if (elections.hasWon(total)
                            && state.role() == Role.CANDIDATE
                            && state.currentTerm() == term) {
                        becomeLeader();
                    }
                }
            });
        }
    }

    /** Transition to leader and initialize per-peer replication bookmarks. */
    private void becomeLeader() {
        state.becomeLeader();
        long nextIdx = log.lastIndex() + 1;
        synchronized (nextIndex) {
            nextIndex.clear();
            for (NodeId peer : config.peers()) {
                nextIndex.put(peer, nextIdx);
            }
        }
    }

    // ---------- RPC handlers (called by RequestHandler for incoming lines) ----------

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

    // ---------- Election timing ----------

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

    // ---------- Accessors (useful for tests and the server) ----------

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