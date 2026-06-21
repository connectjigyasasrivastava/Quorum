package com.jigyasa.quorum;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads messages back from an append-only log file written by SegmentWriter.
 * Used on startup to rebuild in-memory state from disk (recovery).
 */
public class SegmentReader {

    private final Path file;
    private final String topic;

    public SegmentReader(Path file, String topic) {
        this.file = file;
        this.topic = topic;
    }

    /**
     * Reads all messages from the log file in order.
     * Returns an empty list if the file does not exist.
     */
    public List<Message> readAll() throws IOException {
        List<Message> messages = new ArrayList<>();
        if (!Files.exists(file)) {
            return messages;
        }

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            while (true) {
                try {
                    long id = in.readLong();
                    long timestamp = in.readLong();
                    int length = in.readInt();
                    byte[] payload = new byte[length];
                    in.readFully(payload);
                    messages.add(new Message(id, topic, payload, timestamp));
                } catch (EOFException e) {
                    break; // reached end of file
                }
            }
        }
        return messages;
    }
}