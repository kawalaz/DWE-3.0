# ADR-0003: Reliability model

**Status:** Accepted (DWE 0.3.0)

## Context
0.2.0 had seven logical channels but they were routing labels only. A game needs different guarantees on
different data: commands and inventory changes must arrive exactly once and in order; snapshots and inputs are
worthless once stale and must never be delayed by a retransmission.

## Decision
* The delivery mechanism of each channel is a **protocol constant** (`channel_delivery()`), not negotiated:
  Control, Combat, Inventory, Destruction, WorldSync = `ReliableOrdered`; Input = `Unreliable` with a 64-wide
  duplicate/too-old window; Snapshot = `UnreliableSequenced` (newest wins). Changing a mapping is a protocol change.
* Reliability is **message-level, per channel**: each channel has its own sequence space, window and reorder buffer,
  so a lost message on Combat never blocks Control (no cross-channel head-of-line blocking).
* Acknowledgement = cumulative `next_expected` + 64-bit selective bitmap, one `Ack` packet carrying all channels.
  Delayed only until the end of the current receive batch; a duplicate triggers a re-ack.
* Retransmission = RFC 6298 RTO with exponential backoff, Karn's rule, and fast retransmit after 3 informative acks.
* Failure = N retransmissions **without acknowledgement progress** (not per message): a lossy but alive link must
  never be abandoned, a dead one must be detected in bounded time.
* Everything is bounded: window 64, backlog 256 (`QueueFull` back-pressure), reorder buffer 64 slots / 32 KiB.
* The state machines take time as a parameter and own no sockets, so the full stack runs deterministically in a simulator.
* `Hello` carries a client nonce so that a restarted client on the same endpoint gets a fresh session; otherwise the
  stale per-channel sequence state would silently swallow its messages.
* Reply-class messages are never answered (no ping-pong loops); rate-limited datagrams get no reply.

## Alternatives considered
* **Packet-level acks piggybacked on every datagram (Fiedler style):** fewer packets, but couples all channels and
  makes ordered redelivery harder to reason about. Kept as a possible optimisation.
* **A single reliable ordered stream for everything:** simple but re-introduces head-of-line blocking between channels.
* **Per-message retry budget:** rejected after load testing - it abandons healthy-but-lossy links.
* **Delegating to GameNetworkingSockets' reliability:** see ADR-0004; the DWE layer is kept above every transport so
  that transports are comparable and the behaviour is identical.

## Consequences
* No fragmentation: a message must fit one datagram (<= 1179 bytes). Large state must be split by the sender.
* No congestion control yet; the window and the rate limiter only bound the load.
* A reliable-channel failure closes the whole session.
