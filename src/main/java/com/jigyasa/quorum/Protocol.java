package com.jigyasa.quorum;

/**
 * Defines the wire protocol shared by the server and clients.
 *
 * Every request and response is a single line of UTF-8 text terminated
 * by a newline. Fields are separated by a single space.
 *
 * Requests:
 *   CREATE <topic>
 *   PRODUCE <topic> <base64-payload>
 *   CONSUME <topic> <offset>
 *   SIZE <topic>
 *
 * Responses:
 *   OK <value>        success, with an optional value
 *   ERROR <message>   failure, with a reason
 *   EMPTY             nothing at the requested offset
 */
public final class Protocol {

    public static final String CREATE = "CREATE";
    public static final String PRODUCE = "PRODUCE";
    public static final String CONSUME = "CONSUME";
    public static final String SIZE = "SIZE";

    public static final String OK = "OK";
    public static final String ERROR = "ERROR";
    public static final String EMPTY = "EMPTY";

    private Protocol() {
        // utility class, no instances
    }
}