package com.jigyasa.quorum;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A message queue that persists every message to disk and rebuilds
 * its in-memory state from disk on startup (durability + recovery).
 *
 * Each topic is stored in its own append-only log file:
 *   <dataDir>/<topic>.log
 *
 * For simplicity this version uses a single partition per topic.
 */
public class PersistentMessageQueue implements AutoCloseable {

    private final Path dataDir;
    private final ConcurrentHashMap<String, Partition> partitions;
    private final ConcurrentHashMap<String, SegmentWriter> writers;
    private final AtomicLong idGenerator;

    public PersistentMessageQueue(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.partitions = new ConcurrentHashMap<>();
        this.writers = new ConcurrentHashMap<>();
        this.idGenerator = new AtomicLong(0);
        Files.createDirectories(dataDir);
    }

    private Path logFile(String topic) {
        return dataDir.resolve(topic + ".log");
    }

    /**
     * Creates a topic. If a log file already exists, replays it into memory.
     */
    public void createTopic(String topic) {
        partitions.computeIfAbsent(topic, t -> {
            Partition partition = new Partition(0);
            try {
                List<Message> existing = new SegmentReader(logFile(t), t).readAll();
                long maxId = 0;
                for (Message m : existing) {
                    partition.append(m);
                    maxId = Math.max(maxId, m.id());
                }
                final long finalMaxId = maxId;
                idGenerator.updateAndGet(current -> Math.max(current, finalMaxId));
                writers.put(t, new SegmentWriter(logFile(t)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return partition;
        });
    }

    /**
     * Produces a message: writes to disk first, then updates memory.
     * Returns the offset.
     */
    public synchronized long produce(String topic, byte[] payload) throws IOException {
        Partition partition = partitions.get(topic);
        SegmentWriter writer = writers.get(topic);
        if (partition == null || writer == null) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }
        long id = idGenerator.incrementAndGet();
        Message message = Message.of(id, topic, payload);
        writer.append(message);          // durable on disk first
        return partition.append(message); // then in memory
    }

    public Message consume(String topic, long offset) {
        Partition partition = partitions.get(topic);
        if (partition == null) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }
        return partition.read(offset);
    }

    public long size(String topic) {
        Partition partition = partitions.get(topic);
        if (partition == null) {
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }
        return partition.size();
    }

    @Override
    public void close() throws IOException {
        for (SegmentWriter writer : writers.values()) {
            writer.close();
        }
    }
}
