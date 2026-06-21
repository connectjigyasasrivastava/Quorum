package com.jigyasa.quorum;

/**
 * A topic is a named stream of messages, split across one or more partitions.
 * Producers send to a topic; the topic routes each message to a partition.
 */
public class Topic {

    private final String name;
    private final Partition[] partitions;

    public Topic(String name, int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        this.name = name;
        this.partitions = new Partition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new Partition(i);
        }
    }

    /**
     * Chooses a partition for a message key using hash-based routing.
     * Same key always lands on the same partition (ordering guarantee per key).
     */
    public int partitionForKey(String key) {
        int hash = (key == null) ? 0 : Math.abs(key.hashCode());
        return hash % partitions.length;
    }

    public Partition getPartition(int index) {
        return partitions[index];
    }

    public int getPartitionCount() {
        return partitions.length;
    }

    public String getName() {
        return name;
    }
}