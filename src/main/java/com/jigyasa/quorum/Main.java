package com.jigyasa.quorum;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Entry point for running a Quorum server from the command line.
 *
 * Usage:
 *   java -jar quorum.jar [port] [dataDir]
 *
 * Defaults: port 9092, data directory "./quorum-data".
 */
public class Main {

    private static final int DEFAULT_PORT = 9092;
    private static final String DEFAULT_DATA_DIR = "quorum-data";

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Path dataDir = Path.of(args.length > 1 ? args[1] : DEFAULT_DATA_DIR);

        QuorumServer server = new QuorumServer(port, dataDir);

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
}