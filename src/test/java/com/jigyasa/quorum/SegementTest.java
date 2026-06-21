package com.jigyasa.quorum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentTest {

    @Test
    void writeThenReadBackSingleMessage(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("segment.log");

        Message original = Message.of(1, "orders", "hello".getBytes());
        try (SegmentWriter writer = new SegmentWriter(file)) {
            writer.append(original);
        }

        List<Message> read = new SegmentReader(file, "orders").readAll();

        assertEquals(1, read.size());
        assertEquals(1, read.get(0).id());
        assertEquals("hello", new String(read.get(0).payload()));
    }

    @Test
    void writeMultipleMessagesPreservesOrder(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("segment.log");

        try (SegmentWriter writer = new SegmentWriter(file)) {
            writer.append(Message.of(1, "logs", "a".getBytes()));
            writer.append(Message.of(2, "logs", "b".getBytes()));
            writer.append(Message.of(3, "logs", "c".getBytes()));
        }

        List<Message> read = new SegmentReader(file, "logs").readAll();

        assertEquals(3, read.size());
        assertEquals("a", new String(read.get(0).payload()));
        assertEquals("b", new String(read.get(1).payload()));
        assertEquals("c", new String(read.get(2).payload()));
    }

    @Test
    void appendsSurviveWriterReopen(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("segment.log");

        try (SegmentWriter writer = new SegmentWriter(file)) {
            writer.append(Message.of(1, "t", "first".getBytes()));
        }
        try (SegmentWriter writer = new SegmentWriter(file)) {
            writer.append(Message.of(2, "t", "second".getBytes()));
        }

        List<Message> read = new SegmentReader(file, "t").readAll();

        assertEquals(2, read.size());
        assertEquals("first", new String(read.get(0).payload()));
        assertEquals("second", new String(read.get(1).payload()));
    }

    @Test
    void readingMissingFileReturnsEmpty(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("does-not-exist.log");
        List<Message> read = new SegmentReader(file, "t").readAll();
        assertEquals(0, read.size());
    }
}