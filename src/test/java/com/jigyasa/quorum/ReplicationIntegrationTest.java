package com.jigyasa.quorum;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationIntegrationTest {

    private final List<QuorumServer> servers = new ArrayList<>();

    @AfterEach
    void tearDown() throws IOException {
        for (QuorumServer s : servers) {
            s.close();
        }
        servers.clear();
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
        return server;
    }

    private List<QuorumServer> startThreeNodeCluster(Path baseDir) throws IOException {
        NodeId n1 = new NodeId("localhost", 19192);
        NodeId n2 = new NodeId("localhost", 19193);
        NodeId n3 = new NodeId("localhost", 19194);

        QuorumServer s1 = startNode(19192, baseDir.resolve("d1"), n1, List.of(n2, n3));
        QuorumServer s2 = startNode(19193, baseDir.resolve("d2"), n2, List.of(n1, n3));
        QuorumServer s3 = startNode(19194, baseDir.resolve("d3"), n3, List.of(n1, n2));

        return List.of(s1, s2, s3);
    }

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
    void clientWriteReplicatesAndCommits(@TempDir Path baseDir) throws Exception {
        List<QuorumServer> cluster = startThreeNodeCluster(baseDir);

        QuorumServer leader = awaitSingleLeader(cluster, 5000);
        assertNotNull(leader, "a leader must be elected first");

        RaftNode leaderRaft = leader.raft();

        // Submit several client writes through the leader's replicated log.
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        long lastIndex = -1;
        for (int i = 0; i < 3; i++) {
            lastIndex = leaderRaft.appendClientEntry("orders", payload);
            assertTrue(lastIndex >= 0, "leader should accept the client write");
        }

        // Give the heartbeat loop time to replicate and advance commit index.
        long target = lastIndex;
        long deadline = System.currentTimeMillis() + 5000;
        boolean committed = false;
        while (System.currentTimeMillis() < deadline) {
            if (leaderRaft.commitIndex() >= target) {
                committed = true;
                break;
            }
            Thread.sleep(50);
        }

        assertTrue(committed,
                "leader commit index should reach the last written entry once a quorum replicates");
    }

    @Test
    void followersReceiveReplicatedEntries(@TempDir Path baseDir) throws Exception {
        List<QuorumServer> cluster = startThreeNodeCluster(baseDir);

        QuorumServer leader = awaitSingleLeader(cluster, 5000);
        assertNotNull(leader, "a leader must be elected first");

        byte[] payload = "data".getBytes(StandardCharsets.UTF_8);
        long writtenIndex = leader.raft().appendClientEntry("events", payload);
        assertTrue(writtenIndex >= 0);

        // Each follower's log should catch up to at least the written index.
        long deadline = System.currentTimeMillis() + 5000;
        boolean allCaughtUp = false;
        while (System.currentTimeMillis() < deadline) {
            boolean ok = true;
            for (QuorumServer s : cluster) {
                if (s == leader) {
                    continue;
                }
                if (s.raft().log().lastIndex() < writtenIndex) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                allCaughtUp = true;
                break;
            }
            Thread.sleep(50);
        }

        assertTrue(allCaughtUp,
                "followers should replicate the leader's entry into their own logs");
    }
}