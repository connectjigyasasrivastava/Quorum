package com.jigyasa.quorum;

import java.io.IOException;
import java.util.Base64;

/**
 * Translates a single text request line into an action on the queue
 * and returns a single text response line.
 *
 * This class is transport-agnostic: it knows nothing about sockets.
 * It just maps protocol strings to queue operations, which makes it
 * easy to test without any networking.
 */
public class RequestHandler {

    private final PersistentMessageQueue queue;

    public RequestHandler(PersistentMessageQueue queue) {
        this.queue = queue;
    }

    /**
     * Handles one request line and returns one response line (no newline).
     */
    public String handle(String line) {
        if (line == null || line.isBlank()) {
            return Protocol.ERROR + " empty request";
        }

        String[] parts = line.trim().split(" ");
        String command = parts[0];

        try {
            switch (command) {
                case Protocol.CREATE -> {
                    requireArgs(parts, 2);
                    queue.createTopic(parts[1]);
                    return Protocol.OK + " created";
                }
                case Protocol.PRODUCE -> {
                    requireArgs(parts, 3);
                    byte[] payload = Base64.getDecoder().decode(parts[2]);
                    long offset = queue.produce(parts[1], payload);
                    return Protocol.OK + " " + offset;
                }
                case Protocol.CONSUME -> {
                    requireArgs(parts, 3);
                    long offset = Long.parseLong(parts[2]);
                    Message message = queue.consume(parts[1], offset);
                    if (message == null) {
                        return Protocol.EMPTY;
                    }
                    String encoded = Base64.getEncoder().encodeToString(message.payload());
                    return Protocol.OK + " " + encoded;
                }
                case Protocol.SIZE -> {
                    requireArgs(parts, 2);
                    return Protocol.OK + " " + queue.size(parts[1]);
                }
                default -> {
                    return Protocol.ERROR + " unknown command: " + command;
                }
            }
        } catch (IllegalArgumentException e) {
            return Protocol.ERROR + " " + e.getMessage();
        } catch (IOException e) {
            return Protocol.ERROR + " io failure: " + e.getMessage();
        }
    }

    private void requireArgs(String[] parts, int expected) {
        if (parts.length < expected) {
            throw new IllegalArgumentException("expected " + expected + " arguments");
        }
    }
}