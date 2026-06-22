package com.jigyasa.quorum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * A simple client for talking to a QuorumServer over TCP.
 *
 * One request line is sent; one response line is read back.
 * Payloads are Base64-encoded on the wire so binary data is safe.
 */
public class QuorumClient implements AutoCloseable {

    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;

    public QuorumClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = new PrintWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
    }

    private String send(String request) throws IOException {
        out.println(request);
        String response = in.readLine();
        if (response == null) {
            throw new IOException("server closed connection");
        }
        return response;
    }

    public void createTopic(String topic) throws IOException {
        String response = send(Protocol.CREATE + " " + topic);
        expectOk(response);
    }

    /**
     * Produces a message and returns the offset it was stored at.
     */
    public long produce(String topic, byte[] payload) throws IOException {
        String encoded = Base64.getEncoder().encodeToString(payload);
        String response = send(Protocol.PRODUCE + " " + topic + " " + encoded);
        expectOk(response);
        return Long.parseLong(response.substring(3));
    }

    /**
     * Consumes a message at an offset. Returns null if nothing is there.
     */
    public byte[] consume(String topic, long offset) throws IOException {
        String response = send(Protocol.CONSUME + " " + topic + " " + offset);
        if (response.equals(Protocol.EMPTY)) {
            return null;
        }
        expectOk(response);
        return Base64.getDecoder().decode(response.substring(3));
    }

    public long size(String topic) throws IOException {
        String response = send(Protocol.SIZE + " " + topic);
        expectOk(response);
        return Long.parseLong(response.substring(3));
    }

    private void expectOk(String response) throws IOException {
        if (!response.startsWith(Protocol.OK)) {
            throw new IOException("server error: " + response);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}