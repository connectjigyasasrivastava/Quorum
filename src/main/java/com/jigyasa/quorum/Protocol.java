package com.jigyasa.quorum;

/**
 * Defines the wire protocol shared by the server and clients.
 *
 * Every request and response is a single line of UTF-8 text terminated
 * by a newline. Fields are separated by a single space.
 *
 * --- Queue requests (Phase 1-3) ---
 *   CREATE <topic>
 *   PRODUCE <topic> <base64-payload>
 *   CONSUME <topic> <offset>
 *   SIZE <topic>
 *
 * --- Raft requests (Phase 4) ---
 *   VOTE <term> <candidateHost:port> <lastLogIndex> <lastLogTerm>
 *   APPEND <term> <leaderHost:port> <prevIndex> <prevTerm> <leaderCommit> <entryCount> [<entry> ...]
 *
 *   Each replicated entry is encoded as a single token:
 *     <entryTerm>,<entryIndex>,<topic>,<base64-payload>
 *   An APPEND with entryCount 0 is a heartbeat.
 *
 * --- Responses ---
 *   OK <value>            success, with an optional value
 *   ERROR <message>       failure, with a reason
 *   EMPTY                 nothing at the requested offset
 *   VOTERESULT <term> <granted:true|false>
 *   APPENDRESULT <term> <success:true|false> <matchIndex>
 */
public final class Protocol {

    // Queue commands
    public static final String CREATE = "CREATE";
    public static final String PRODUCE = "PRODUCE";
    public static final String CONSUME = "CONSUME";
    public static final String SIZE = "SIZE";

    // Raft commands
    public static final String VOTE = "VOTE";
    public static final String APPEND = "APPEND";

    // Responses
    public static final String OK = "OK";
    public static final String ERROR = "ERROR";
    public static final String EMPTY = "EMPTY";
    public static final String VOTERESULT = "VOTERESULT";
    public static final String APPENDRESULT = "APPENDRESULT";

    private Protocol() {
        // utility class, no instances
    }
}