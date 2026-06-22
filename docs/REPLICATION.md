# Replication and Leader Election

Quorum replicates its log across multiple nodes using a Raft-inspired design.
This document explains the moving parts built in Phase 4.

## Roles

Every node is in one of three roles:

- **Leader** — accepts client writes and replicates them to followers.
- **Follower** — receives replicated entries from the leader and applies them.
- **Candidate** — a follower that has timed out and is running an election.

## Terms

Time is divided into numbered **terms**. Each term has at most one leader.
A higher term always wins: any node that sees a higher term steps down to
follower and adopts that term. This prevents two leaders from coexisting.

## The replicated log

Each entry carries a term, an index, a topic, and a payload. Followers accept
new entries only when they line up with their existing log:

- the entry just before the new ones (prevIndex) must exist, and
- its term (prevTerm) must match.

If a follower's log conflicts with the leader's, the conflicting tail is
truncated and replaced. This guarantees all logs converge to the same prefix.

## Commit index

An entry is **committed** once it is stored on a quorum of nodes
(floor(N/2) + 1). The leader tracks how far each follower has replicated
(matchIndex) and advances the commit index to the highest index present on a
majority. Only committed entries are applied and acknowledged to clients.

## Elections

When a follower stops hearing from the leader, it becomes a candidate,
increments the term, votes for itself, and requests votes from peers. A peer
grants its vote only if:

- the candidate's term is at least the peer's own,
- the peer has not already voted for someone else this term, and
- the candidate's log is at least as up-to-date as the peer's.

A candidate that collects a quorum of votes becomes the new leader.

## Safety properties

- **Election safety** — at most one leader per term.
- **Log matching** — if two logs share an entry at the same index and term,
  all preceding entries are identical.
- **Leader completeness** — a committed entry is present in every future
  leader's log, because only up-to-date candidates can win.
