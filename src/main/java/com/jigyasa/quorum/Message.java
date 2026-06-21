package com.jigyasa.quorum;

/**
 * Represents a single message in the Quorum message queue.
 * Immutable once created.
 */
public record Message(long id, String topic, byte[] payload, long timestamp) {

    public static Message of(long id, String topic, byte[] payload) {
        return new Message(id, topic, payload, System.currentTimeMillis());
    }
}