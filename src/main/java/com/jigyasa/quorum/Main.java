package com.jigyasa.quorum;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for running a Quorum server.
 *
 * Standalone (Phase 1-3):
 *   java -jar quorum.jar [port] [dataDir]
 *
 * Replicated (Phase 4):
 *   java -jar quorum.jar [port] [dataDir] [self] [peer1,peer2,...]
 *
 *   self  : this node's address, host:port
 *   peers : comma-separated host:port list of the other nodes
 *
 * Defaults: port 9092, data directory "quorum-data".
 */
public class Main {

    private static final int DEFAULT_PORT = 9092;
    private static final String DEFAULT_DATA_DIR = "quorum-data";

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Path dataDir = Path.of(args.length > 1 ? args[1] : DEFAULT_DATA_DIR);

        QuorumServer server;
        if (args.length > 3) {
            NodeId self = NodeId.parse(args[2]);
            List<NodeId> peers = parsePeers(args[3]);
            server = new QuorumServer(port, dataDir, self, peers);
        } else {
            server = new QuorumServer(port, dataDir);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
                System.out.println("Quorum server stopped.");
            } catch (IOException e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));

        server.start();
    }

    private static List<NodeId> parsePeers(String csv) {
        List<NodeId> peers = new ArrayList<>();
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                peers.add(NodeId.parse(trimmed));
            }
        }
        return peers;
    }
}