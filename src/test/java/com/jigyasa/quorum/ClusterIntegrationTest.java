package com.jigyasa.quorum;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterIntegrationTest {

    private final List<QuorumServer> servers = new ArrayList<>();
    private final List<Thread> serverThreads = new ArrayList<>();

    @AfterEach
    void tearDown() throws IOException {
        for (QuorumServer s : servers) {
            s.close();
        }
        servers.clear();
        serverThreads.clear();
    }

    private QuorumServer startNode(int port, Path dataDir, NodeId self, List<NodeId> peers) throws IOException {
        QuorumServer server = new QuorumServer(port, dataDir, self, peers);
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // socket closed on shutdown; ignore
            }
        }, "server-" + port);
        t.setDaemon(true);
        t.start();
        servers.add(server);
        serverThreads.add(t);
        return server;
    }

    private List<QuorumServer> startThreeNodeCluster(Path baseDir) throws IOException {
        NodeId n1 = new NodeId("localhost", 19092);
        NodeId n2 = new NodeId("localhost", 19093);
        NodeId n3 = new NodeId("localhost", 19094);

        QuorumServer s1 = startNode(19092, baseDir.resolve("d1"), n1, List.of(n2, n3));
        QuorumServer s2 = startNode(19093, baseDir.resolve("d2"), n2, List.of(n1, n3));
        QuorumServer s3 = startNode(19094, baseDir.resolve("d3"), n3, List.of(n1, n2));

        return List.of(s1, s2, s3);
    }

    private List<QuorumServer> running() {
        List<QuorumServer> alive = new ArrayList<>();
        for (QuorumServer s : servers) {
            alive.add(s);
        }
        return alive;
    }

    // Polls until exactly one leader exists among the given servers, or fails.
    private QuorumServer awaitSingleLeader(List<QuorumServer> nodes, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<QuorumServer> leaders = new ArrayList<>();
            for (QuorumServer s : nodes) {
                RaftNode r = s.raft();
                if (r != null && r.role() == Role.LEADER) {
                    leaders.add(s);
                }
            }
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(50);
        }
        return null;
    }

    @Test
    void electsExactlyOneLeader(@TempDir Path baseDir) throws Exception {
        List<QuorumServer> cluster = startThreeNodeCluster(baseDir);

        QuorumServer leader = awaitSingleLeader(cluster, 5000);

        assertNotNull(leader, "cluster should elect exactly one leader");
        assertEquals(Role.LEADER, leader.raft().role());

        long leaderTerm = leader.raft().currentTerm();
        assertTrue(leaderTerm >= 1, "leader term should be at least 1");
    }

    @Test
    void newLeaderTakesOverAfterLeaderFails(@TempDir Path baseDir) throws Exception {
        List<QuorumServer> cluster = startThreeNodeCluster(baseDir);

        QuorumServer firstLeader = awaitSingleLeader(cluster, 5000);
        assertNotNull(firstLeader, "initial leader should be elected");
        NodeId fallenId = firstLeader.raft().id();
        long firstTerm = firstLeader.raft().currentTerm();

        // Kill the leader.
        firstLeader.close();

        // The two survivors should elect a new leader.
        List<QuorumServer> survivors = new ArrayList<>();
        for (QuorumServer s : cluster) {
            if (s != firstLeader) {
                survivors.add(s);
            }
        }

        QuorumServer newLeader = awaitSingleLeader(survivors, 5000);

        assertNotNull(newLeader, "a new leader should be elected after failover");
        assertNotEquals(fallenId, newLeader.raft().id(), "new leader must be a different node");
        assertTrue(newLeader.raft().currentTerm() > firstTerm,
                "new leader's term must be higher than the failed leader's");
    }
}