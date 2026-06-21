package com.jigyasa.quorum;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The central broker. Holds all topics and routes produce/consume requests.
 * Thread-safe: many producers and consumers can use it concurrently.
 */
public class MessageQueue {

    private final ConcurrentHashMap<String, Topic> topics;
    private final AtomicLong idGenerator;

    public MessageQueue() {
        this.topics = new ConcurrentHashMap<>();
        this.idGenerator = new AtomicLong(0);
    }

    /**
     * Creates a topic with the given number of partitions.
     * Does nothing if the topic already exists.
     */
    public void createTopic(String name, int partitionCount) {
        topics.computeIfAbsent(name, n -> new Topic(n, partitionCount));
    }

    /**
     * Produces a message to a topic. The key decides the partition.
     * Returns the offset the message was stored at.
     */
    public long produce(String topicName, String key, byte[] payload) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Unknown topic: " + topicName);
        }
        int partitionIndex = topic.partitionForKey(key);
        Partition partition = topic.getPartition(partitionIndex);

        long id = idGenerator.incrementAndGet();
        Message message = Message.of(id, topicName, payload);
        return partition.append(message);
    }

    /**
     * Consumes a single message from a specific topic-partition at an offset.
     * Returns null if nothing exists at that offset yet.
     */
    public Message consume(String topicName, int partitionIndex, long offset) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Unknown topic: " + topicName);
        }
        return topic.getPartition(partitionIndex).read(offset);
    }

    public Topic getTopic(String name) {
        return topics.get(name);
    }
}