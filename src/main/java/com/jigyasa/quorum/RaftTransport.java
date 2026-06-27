package com.jigyasa.quorum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The client side of Raft networking. Given a peer's NodeId, it opens a
 * TCP connection, sends one request line, reads one response line, and
 * parses it back into a typed response.
 *
 * Connections are short-lived (one request each). This is simple and
 * correct; an optimization later would be to pool connections per peer.
 *
 * This class also owns the wire encoding/decoding of Raft messages, so the
 * exact same format is produced here and parsed in RequestHandler.
 */
public class RaftTransport {

    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    public RaftTransport() {
        this(300, 300);
    }

    public RaftTransport(int connectTimeoutMs, int readTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    // ---------- Sending requests ----------

    /**
     * Sends a VoteRequest to a peer and returns its VoteResponse,
     * or null if the peer could not be reached or replied badly.
     */
    public VoteResponse sendVote(NodeId peer, VoteRequest req) {
        String line = encodeVote(req);
        String reply = roundTrip(peer, line);
        return reply == null ? null : parseVoteResult(reply);
    }

    /**
     * Sends a ReplicationRequest to a peer and returns its
     * ReplicationResponse, or null if unreachable / bad reply.
     */
    public ReplicationResponse sendAppend(NodeId peer, ReplicationRequest req) {
        String line = encodeAppend(req);
        String reply = roundTrip(peer, line);
        return reply == null ? null : parseAppendResult(reply);
    }

    /**
     * Opens a socket, writes one line, reads one line back.
     * Returns null on any IO problem (treated as "peer unavailable").
     */
    private String roundTrip(NodeId peer, String requestLine) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.host(), peer.port()), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            try (PrintWriter out = new PrintWriter(
                    new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                out.println(requestLine);
                return in.readLine();
            }
        } catch (IOException e) {
            return null; // peer down or slow; caller treats as no response
        }
    }

    // ---------- Encoding (must match RequestHandler parsing) ----------

    public static String encodeVote(VoteRequest r) {
        return Protocol.VOTE + " "
                + r.term() + " "
                + r.candidateId() + " "
                + r.lastLogIndex() + " "
                + r.lastLogTerm();
    }

    public static String encodeAppend(ReplicationRequest r) {
        StringBuilder sb = new StringBuilder();
        sb.append(Protocol.APPEND).append(' ')
                .append(r.term()).append(' ')
                .append(r.leaderId()).append(' ')
                .append(r.prevIndex()).append(' ')
                .append(r.prevTerm()).append(' ')
                .append(r.leaderCommit()).append(' ')
                .append(r.entries().size());
        for (LogEntry e : r.entries()) {
            sb.append(' ').append(encodeEntry(e));
        }
        return sb.toString();
    }

    /** One entry as a single whitespace-free token: term,index,topic,base64payload */
    public static String encodeEntry(LogEntry e) {
        String payload = Base64.getEncoder().encodeToString(e.payload());
        return e.term() + "," + e.index() + "," + e.topic() + "," + payload;
    }

    public static LogEntry decodeEntry(String token) {
        // Split into at most 4 parts so a topic can't break the payload.
        String[] f = token.split(",", 4);
        long term = Long.parseLong(f[0]);
        long index = Long.parseLong(f[1]);
        String topic = f[2];
        byte[] payload = Base64.getDecoder().decode(f[3]);
        return new LogEntry(term, index, topic, payload);
    }

    public static List<LogEntry> decodeEntries(String[] parts, int startIdx, int count) {
        java.util.List<LogEntry> result = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(decodeEntry(parts[startIdx + i]));
        }
        return result;
    }

    // ---------- Parsing responses ----------

    private VoteResponse parseVoteResult(String line) {
        String[] p = line.trim().split(" ");
        if (p.length < 3 || !p[0].equals(Protocol.VOTERESULT)) {
            return null;
        }
        return new VoteResponse(Long.parseLong(p[1]), Boolean.parseBoolean(p[2]));
    }

    private ReplicationResponse parseAppendResult(String line) {
        String[] p = line.trim().split(" ");
        if (p.length < 4 || !p[0].equals(Protocol.APPENDRESULT)) {
            return null;
        }
        return new ReplicationResponse(
                Long.parseLong(p[1]),
                Boolean.parseBoolean(p[2]),
                Long.parseLong(p[3]));
    }
}