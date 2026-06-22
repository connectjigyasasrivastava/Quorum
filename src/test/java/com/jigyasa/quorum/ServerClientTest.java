package com.jigyasa.quorum;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ServerClientTest {

    private QuorumServer server;
    private Thread serverThread;
    private int port;

    @BeforeEach
    void startServer(@TempDir Path dir) throws IOException, InterruptedException {
        // port 0 = let the OS pick a free port
        server = new QuorumServer(0, dir);
        serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException ignored) {
            }
        });
        serverThread.start();

        // wait until the server has bound to a port
        for (int i = 0; i < 50 && server.getPort() == 0; i++) {
            Thread.sleep(20);
        }
        port = server.getPort();
    }

    @AfterEach
    void stopServer() throws IOException {
        server.close();
    }

    @Test
    void produceAndConsumeOverTcp() throws IOException {
        try (QuorumClient client = new QuorumClient("localhost", port)) {
            client.createTopic("orders");
            long offset = client.produce("orders", "hello".getBytes());
            assertEquals(0, offset);

            byte[] payload = client.consume("orders", 0);
            assertEquals("hello", new String(payload));
        }
    }

    @Test
    void multipleMessagesOverTcp() throws IOException {
        try (QuorumClient client = new QuorumClient("localhost", port)) {
            client.createTopic("logs");
            client.produce("logs", "a".getBytes());
            client.produce("logs", "b".getBytes());
            client.produce("logs", "c".getBytes());

            assertEquals(3, client.size("logs"));
            assertEquals("a", new String(client.consume("logs", 0)));
            assertEquals("b", new String(client.consume("logs", 1)));
            assertEquals("c", new String(client.consume("logs", 2)));
        }
    }

    @Test
    void consumeEmptyReturnsNull() throws IOException {
        try (QuorumClient client = new QuorumClient("localhost", port)) {
            client.createTopic("empty");
            assertNull(client.consume("empty", 0));
        }
    }

    @Test
    void twoClientsShareSameQueue() throws IOException {
        try (QuorumClient writer = new QuorumClient("localhost", port)) {
            writer.createTopic("shared");
            writer.produce("shared", "fromWriter".getBytes());
        }
        try (QuorumClient reader = new QuorumClient("localhost", port)) {
            assertEquals("fromWriter", new String(reader.consume("shared", 0)));
        }
    }
}