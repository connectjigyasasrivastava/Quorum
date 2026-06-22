package com.jigyasa.quorum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A TCP server that exposes the message queue over the network.
 *
 * Each client connection is handled on its own thread. Each request is
 * one line of text; each response is one line of text (see Protocol).
 */
public class QuorumServer implements AutoCloseable {

    private final int port;
    private final RequestHandler handler;
    private final ExecutorService clientPool;
    private ServerSocket serverSocket;
    private volatile boolean running;

    public QuorumServer(int port, Path dataDir) throws IOException {
        this.port = port;
        PersistentMessageQueue queue = new PersistentMessageQueue(dataDir);
        this.handler = new RequestHandler(queue);
        this.clientPool = Executors.newCachedThreadPool();
    }

    /**
     * Starts listening. Blocks the calling thread accepting connections
     * until close() is called.
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        System.out.println("Quorum server listening on port " + port);

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

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        clientPool.shutdownNow();
    }
}