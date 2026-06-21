package com.jigyasa.quorum;

import java.util.ArrayList;
import java.util.List;

/**
 * A partition is an ordered, append-only sequence of messages.
 * Each message gets an offset = its position in the partition.
 * Thread-safe for concurrent producers and consumers.
 */
public class Partition {

    private final int partitionId;
    private final List<Message> log;

    public Partition(int partitionId) {
        this.partitionId = partitionId;
        this.log = new ArrayList<>();
    }

    public synchronized long append(Message message) {
        long offset = log.size();
        log.add(message);
        return offset;
    }

    public synchronized Message read(long offset) {
        if (offset < 0 || offset >= log.size()) {
            return null;
        }
        return log.get((int) offset);
    }

    public synchronized long size() {
        return log.size();
    }

    public int getPartitionId() {
        return partitionId;
    }
}
