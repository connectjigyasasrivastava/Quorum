package com.jigyasa.quorum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QuorumServer implements AutoCloseable {

    private final int port;
    private final RequestHandler handler;
    private final RaftNode raft;
    private final ExecutorService clientPool;
    private ServerSocket serverSocket;
    private volatile boolean running;

    // Phase 1-3 server: standalone queue, no replication.
    public QuorumServer(int port, Path dataDir) throws IOException {
        this.port = port;
        PersistentMessageQueue queue = new PersistentMessageQueue(dataDir);
        this.raft = null;
        this.handler = new RequestHandler(queue);
        this.clientPool = Executors.newCachedThreadPool();
    }

    // Phase 4 server: replicated. self is this node, peers are the others.
    public QuorumServer(int port, Path dataDir, NodeId self, List<NodeId> peers) throws IOException {
        this.port = port;
        PersistentMessageQueue queue = new PersistentMessageQueue(dataDir);
        ClusterConfig config = new ClusterConfig(self, peers);
        ReplicatedLog log = new ReplicatedLog();
        RaftTransport transport = new RaftTransport();
        this.raft = new RaftNode(config, log, transport);
        this.handler = new RequestHandler(queue, raft);
        this.clientPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        System.out.println("Quorum server listening on port " + port);

        if (raft != null) {
            raft.start();
            System.out.println("Raft node started: " + raft.id());
        }

        while (running) {
            try {
                Socket client = serverSocket.accept();
                clientPool.submit(() -> handleClient(client));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Accept failed: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(
                     new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                String response = handler.handle(line);
                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    public RaftNode raft() {
        return raft;
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (raft != null) {
            raft.stop();
        }
        if (serverSocket != null) {
            serverSocket.close();
        }
        clientPool.shutdownNow();
    }
}