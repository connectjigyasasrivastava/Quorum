package com.jigyasa.quorum;

/**
 * Identifies a single node in a Quorum cluster by host and port.
 * Immutable. Two nodes are equal if host and port match.
 */
public record NodeId(String host, int port) {

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Parses a "host:port" string into a NodeId.
     */
    public static NodeId parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("invalid node address: " + address);
        }
        String host = address.substring(0, colon);
        int port = Integer.parseInt(address.substring(colon + 1));
        return new NodeId(host, port);
    }
}