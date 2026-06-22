package com.jigyasa.quorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Describes the static membership of a Quorum cluster: this node plus its
 * peers. Used to compute quorum size and iterate over peers for replication
 * and elections.
 */
public class ClusterConfig {

    private final NodeId self;
    private final List<NodeId> peers;

    public ClusterConfig(NodeId self, List<NodeId> peers) {
        this.self = self;
        this.peers = new ArrayList<>(peers);
    }

    public NodeId self() {
        return self;
    }

    public List<NodeId> peers() {
        return Collections.unmodifiableList(peers);
    }

    /**
     * Total number of nodes in the cluster (self + peers).
     */
    public int clusterSize() {
        return peers.size() + 1;
    }

    /**
     * The number of nodes that must agree for a decision to commit.
     * For a cluster of N nodes, quorum is floor(N/2) + 1 (a strict majority).
     */
    public int quorum() {
        return (clusterSize() / 2) + 1;
    }
}