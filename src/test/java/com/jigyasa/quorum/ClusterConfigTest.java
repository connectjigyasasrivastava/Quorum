package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterConfigTest {

    @Test
    void singleNodeClusterHasQuorumOfOne() {
        ClusterConfig config = new ClusterConfig(
                new NodeId("localhost", 9092), List.of());
        assertEquals(1, config.clusterSize());
        assertEquals(1, config.quorum());
    }

    @Test
    void threeNodeClusterHasQuorumOfTwo() {
        ClusterConfig config = new ClusterConfig(
                new NodeId("localhost", 9092),
                List.of(new NodeId("localhost", 9093),
                        new NodeId("localhost", 9094)));
        assertEquals(3, config.clusterSize());
        assertEquals(2, config.quorum());
    }

    @Test
    void fiveNodeClusterHasQuorumOfThree() {
        ClusterConfig config = new ClusterConfig(
                new NodeId("localhost", 9092),
                List.of(new NodeId("localhost", 9093),
                        new NodeId("localhost", 9094),
                        new NodeId("localhost", 9095),
                        new NodeId("localhost", 9096)));
        assertEquals(5, config.clusterSize());
        assertEquals(3, config.quorum());
    }
}