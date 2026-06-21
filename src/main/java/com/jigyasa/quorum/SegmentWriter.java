package com.jigyasa.quorum;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Writes messages to disk as an append-only log file.
 *
 * Record format on disk (per message):
 *   [8 bytes id][8 bytes timestamp][4 bytes payloadLength][payload bytes]
 *
 * Appends are flushed to the OS so data survives a process crash.
 */
public class SegmentWriter implements AutoCloseable {

    private final DataOutputStream out;

    public SegmentWriter(Path file) throws IOException {
        // append = true so restarts continue the existing log
        this.out = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(file.toFile(), true)));
    }

    /**
     * Appends one message to the log and flushes it to the OS.
     */
    public synchronized void append(Message message) throws IOException {
        out.writeLong(message.id());
        out.writeLong(message.timestamp());
        out.writeInt(message.payload().length);
        out.write(message.payload());
        out.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }
}