package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageQueueTest {

    @Test
    void produceAndConsumeSingleMessage() {
        MessageQueue mq = new MessageQueue();
        mq.createTopic("orders", 1);

        byte[] payload = "hello".getBytes();
        long offset = mq.produce("orders", "key1", payload);

        assertEquals(0, offset);

        Message consumed = mq.consume("orders", 0, 0);
        assertEquals("orders", consumed.topic());
        assertEquals("hello", new String(consumed.payload()));
    }

    @Test
    void offsetsIncreaseInOrder() {
        MessageQueue mq = new MessageQueue();
        mq.createTopic("logs", 1);

        long first = mq.produce("logs", "k", "a".getBytes());
        long second = mq.produce("logs", "k", "b".getBytes());

        assertEquals(0, first);
        assertEquals(1, second);
    }

    @Test
    void sameKeyGoesToSamePartition() {
        MessageQueue mq = new MessageQueue();
        mq.createTopic("events", 4);

        Topic topic = mq.getTopic("events");
        int p1 = topic.partitionForKey("user-42");
        int p2 = topic.partitionForKey("user-42");

        assertEquals(p1, p2);
    }

    @Test
    void consumingEmptyOffsetReturnsNull() {
        MessageQueue mq = new MessageQueue();
        mq.createTopic("empty", 1);

        Message result = mq.consume("empty", 0, 0);
        assertNull(result);
    }

    @Test
    void producingToUnknownTopicThrows() {
        MessageQueue mq = new MessageQueue();
        assertThrows(IllegalArgumentException.class,
                () -> mq.produce("ghost", "k", "x".getBytes()));
    }
}
