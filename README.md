# Quorum

A distributed, log-based message queue built from scratch in **Java 21** — a simplified Kafka with **Raft-style consensus** for leader election, log replication, and crash failover.

Quorum is built in four phases, from a single-node in-memory queue up to a multi-node replicated cluster that survives leader failure. Every layer is covered by tests, including end-to-end integration tests that spin up a real three-node cluster over TCP and verify election, replication, and failover.

---

## Highlights

- **Append-only log** with topics and partitions, modeled on Kafka's storage design.
- **Disk persistence** via segment files with crash recovery on restart.
- **Custom TCP wire protocol** — single-line, UTF-8, text-based request/response.
- **Raft-style consensus**: randomized-timeout leader election, log replication, and quorum-based commit.
- **Failover**: when a leader crashes, the surviving majority elects a new leader at a higher term.
- **57 passing tests**, including real multi-node integration tests over sockets.

---

## Architecture

Quorum is organized as a stack of layers, each building on the one below.

| Layer | Responsibility | Key classes |
|-------|----------------|-------------|
| Core queue | In-memory topics, partitions, offsets | `Message`, `Partition`, `Topic`, `MessageQueue` |
| Persistence | Durable storage and recovery | `SegmentWriter`, `SegmentReader`, `PersistentMessageQueue` |
| Networking | TCP server, client, wire protocol | `QuorumServer`, `QuorumClient`, `Protocol`, `RequestHandler` |
| Consensus | Election, replication, commit | `RaftNode`, `RaftTransport`, `ElectionManager`, `ReplicationManager`, `CommitTracker`, `ReplicatedLog`, `NodeState`, `ClusterConfig` |

The `RaftNode` is the orchestrator: it owns the election timer, runs the candidate election round, and (when leader) runs the heartbeat and replication loop. The pure decision logic lives in small, independently tested managers (`ElectionManager`, `ReplicationManager`, `CommitTracker`), while `RaftTransport` handles the networking so the logic stays testable without sockets.

---

## Build phases

**Phase 1 — In-memory core.** Topics, partitions, append-only offsets, produce/consume.

**Phase 2 — Disk persistence.** Segment-based storage with write/read and recovery of state on restart.

**Phase 3 — TCP networking.** A multi-threaded server exposes the queue over a text protocol; a matching client speaks it.

**Phase 4 — Raft consensus.** Cluster membership, randomized-timeout leader election, log replication from leader to followers, quorum-based commit index advancement, and leader failover.

---

## Requirements

- Java 21+
- Maven 3.8+

---

## Build & test

```bash
mvn clean package      # compile, run tests, build an executable jar
mvn test               # run the test suite only
```

The build produces an executable fat jar at `target/quorum.jar`.

---

## Running

### Standalone (single node, Phases 1–3)

```bash
java -jar target/quorum.jar [port] [dataDir]
# defaults: port 9092, dataDir "quorum-data"
```

### Replicated cluster (Phase 4)

```bash
java -jar target/quorum.jar <port> <dataDir> <self> <peers>
```

- `self` — this node's address as `host:port`
- `peers` — comma-separated `host:port` list of the other nodes

A three-node local cluster:

```bash
java -jar target/quorum.jar 9092 data1 localhost:9092 localhost:9093,localhost:9094
java -jar target/quorum.jar 9093 data2 localhost:9093 localhost:9092,localhost:9094
java -jar target/quorum.jar 9094 data3 localhost:9094 localhost:9092,localhost:9093
```

The nodes will elect a single leader. Kill the leader process and the remaining two elect a new one.

---

## Wire protocol

Every request and response is one line of UTF-8 text; fields are space-separated.

**Queue commands**

```
CREATE  <topic>
PRODUCE <topic> <base64-payload>
CONSUME <topic> <offset>
SIZE    <topic>
```

**Raft commands**

```
VOTE   <term> <candidateHost:port> <lastLogIndex> <lastLogTerm>
APPEND <term> <leaderHost:port> <prevIndex> <prevTerm> <leaderCommit> <entryCount> [<entry> ...]
```

Each replicated entry is a single whitespace-free token: `term,index,topic,base64payload`. An `APPEND` with `entryCount` 0 is a heartbeat.

**Responses**

```
OK <value>
ERROR <message>
EMPTY
VOTERESULT   <term> <granted>
APPENDRESULT <term> <success> <matchIndex>
```

---

## Testing

The suite covers each layer in isolation plus full-cluster behavior:

- **Unit tests** for the queue, persistence, log, election, replication, and commit logic.
- **`ServerClientTest`** — request/response over a real socket.
- **`ClusterIntegrationTest`** — a three-node cluster elects exactly one leader, then elects a new leader at a higher term after the leader is killed.
- **`ReplicationIntegrationTest`** — a client write replicates to a quorum, the commit index advances, and follower logs catch up.

```bash
mvn test
```

---

## Design notes

- **Randomized election timeouts (150–300 ms)** prevent split votes by ensuring one node usually times out first. The heartbeat interval (50 ms) is well under the minimum election timeout, so a healthy leader keeps followers from timing out.
- **Step down on a higher term.** Any time a node sees a term greater than its own, it reverts to follower — Raft's core safety rule.
- **Commit only on quorum.** An entry is committed only once a majority of nodes have stored it, which is what makes acknowledged writes durable across a leader crash.
- **Log repair via `nextIndex` backoff.** When a follower's log diverges, the leader walks that follower's pointer back until the logs agree, then ships the rest forward.

---

## Roadmap

- Persist Raft state (`currentTerm`, `votedFor`) to disk so a restarted node recovers correctly.
- Log compaction / snapshotting.
- Client redirect-to-leader so writes sent to a follower are forwarded.

---

## License

MIT