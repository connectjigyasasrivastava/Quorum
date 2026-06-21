package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PersistentMessageQueueTest {

    @Test
    void produceAndConsume(@TempDir Path dir) throws IOException {
        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("orders");
            long offset = mq.produce("orders", "hello".getBytes());

            assertEquals(0, offset);
            assertEquals("hello", new String(mq.consume("orders", 0).payload()));
        }
    }

    @Test
    void dataSurvivesRestart(@TempDir Path dir) throws IOException {
        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("orders");
            mq.produce("orders", "first".getBytes());
            mq.produce("orders", "second".getBytes());
        }

        // simulate restart: new instance, same data directory
        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("orders");

            assertEquals(2, mq.size("orders"));
            assertEquals("first", new String(mq.consume("orders", 0).payload()));
            assertEquals("second", new String(mq.consume("orders", 1).payload()));
        }
    }

    @Test
    void idsContinueAfterRestart(@TempDir Path dir) throws IOException {
        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("t");
            mq.produce("t", "a".getBytes());
            mq.produce("t", "b".getBytes());
        }

        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("t");
            mq.produce("t", "c".getBytes());

            assertEquals(3, mq.consume("t", 2).id());
        }
    }

    @Test
    void consumingEmptyOffsetReturnsNull(@TempDir Path dir) throws IOException {
        try (PersistentMessageQueue mq = new PersistentMessageQueue(dir)) {
            mq.createTopic("empty");
            assertNull(mq.consume("empty", 0));
        }
    }
}