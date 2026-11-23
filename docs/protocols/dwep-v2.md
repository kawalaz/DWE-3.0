# DWEP v2

Wire protocol of DWE 0.3.0. All multi-byte integers are **big-endian**. v1 packets are rejected.

## Envelope (unchanged from v1 except the version)

| Field | Size |
|---|---:|
| Magic `DWE1` | 4 |
| Protocol version (**2**) | 1 |
| Message type | 1 |
| Sequence | 4 |
| Payload length | 2 |
| Payload | 0-1188 |

Total datagram size is 12-1200 bytes and the declared payload length must match the datagram exactly.
A datagram larger than 1200 bytes is rejected as a whole (never truncated). Unknown types and versions are rejected.

## Message types

| # | Name | Direction | Payload | `Sequence` field means | Answered? |
|--:|---|---|---|---|---|
| 1 | Ping | C->S | session ID (8) | probe number | Echo |
| 2 | Echo | S->C | same as the Ping | probe number | never |
| 3 | Error | S->C | UTF-8 text | echoes the request | never |
| 4 | Hello | C->S | client nonce (8, non-zero) | handshake transaction ID | Welcome |
| 5 | Welcome | S->C | session ID (8) | echoes the Hello | never |
| 6 | Disconnect | either | session ID (8) + reason (1) | - | DisconnectAck (from the server) |
| 7 | ChannelData | either | session ID (8) + channel (1) + data | **per-channel message sequence** | Ack, on reliable channels |
| 8 | Ack | either | see below | packet counter (diagnostic) | never |
| 9 | DisconnectAck | S->C | session ID (8) | echoes the Disconnect | never |

Reply-class messages (Echo, Error, Welcome, Ack, DisconnectAck) are **never answered**, so two peers can never
bounce packets forever. Datagrams dropped by a rate limiter get no reply at all.

Disconnect reasons: 0 Requested, 1 Timeout, 2 ServerShutdown.

## Handshake

```
client                                   server
  | Hello(seq=T, nonce=N) ------------->  |   creates the session (or finds it: same endpoint + same N)
  |  <------------- Welcome(seq=T, id)    |
```
* The client retransmits the Hello (250 ms, doubling up to 1 s) until the Welcome arrives or it gives up (default 5 s).
* Same endpoint and same nonce -> the same session (idempotent). Same endpoint and a **different** nonce -> the
  client restarted: the old session is replaced and a new ID is issued, so no sequence state is inherited.
* The client pins the server's address and port from the first valid Welcome and ignores any other source afterwards.
* The server answers `Error("server is full")` when at capacity; Hello is rate limited much tighter than other packets.

## Channels and delivery guarantees

The channel's guarantee is part of the protocol (both peers must agree):

| Channel | ID | Delivery | Meaning |
|---|--:|---|---|
| Control | 0 | ReliableOrdered | acknowledged, retransmitted, delivered exactly once, in order |
| Input | 1 | Unreliable | no retransmission; duplicates and datagrams older than a 64-wide window are dropped; delivered in arrival order |
| Snapshot | 2 | UnreliableSequenced | no retransmission; a message is delivered only if newer than every message delivered before ("newest wins") |
| Combat | 3 | ReliableOrdered | as Control |
| Inventory | 4 | ReliableOrdered | as Control |
| Destruction | 5 | ReliableOrdered | as Control |
| WorldSync | 6 | ReliableOrdered | as Control |

Ordering is **per channel**; there is no ordering between channels. A message must fit one datagram
(at most 1179 bytes of application data); there is no fragmentation in v2.

## Sequence numbers

Every channel has an independent 32-bit sequence per direction. The first message has sequence **1**.
Comparison is wrap-safe (serial-number arithmetic: `a` is before `b` if `int32(a - b) < 0`); windows are far
below 2^31 so this is unambiguous. Reliable messages receive their sequence when they enter the send window.

## Ack (type 8)

```
session_id (8) | count (1, 1..7) | count x { channel (1) | next_expected (4) | received_bits (8) }
```
* `next_expected`: every message of that channel with a smaller sequence has been received (cumulative).
* bit `i` of `received_bits`: the message with sequence `next_expected + 1 + i` has also been received (selective).
  Bit 63 is always 0 (the window is 64, so at most `next_expected + 63` can be outstanding).
* Only reliable channels may appear, each at most once; anything else makes the whole Ack invalid.
* An Ack covering messages that were never sent is ignored and counted (`invalid_acks`).

## Reliable-ordered rules

Sender: at most **64** unacknowledged messages in flight; further messages wait in a bounded backlog (256) and
`send` reports `QueueFull` when it is exhausted. A message is retransmitted when its timer expires
(`RTO * 2^retries`, capped) or when three informative acks acknowledged later messages around it (fast
retransmit). RTT is sampled only from messages that were never retransmitted (Karn). The channel - and with it
the session - fails after `max_retransmits` (10) retransmissions **without acknowledgement progress**.

Receiver: delivers `next_expected`, then everything contiguous behind it; buffers out-of-order messages inside the
64-window (and at most 32 KiB); a duplicate of an already delivered message is **re-acknowledged** (the first ack
may have been lost) but not delivered again; anything outside the window is dropped without an ack.

## Liveness

* Client: a Ping every 0.5 s when it has sent nothing; the Echo gives an RTT sample. No packet from the server for
  10 s -> the client fails with "server timeout".
* Server: a session with no packet from its endpoint for 15 s is expired (a Disconnect(Timeout) is sent best effort).
* Disconnect is retransmitted every 250 ms until the DisconnectAck (or 2 s). A retransmitted Disconnect for an
  already closed session is confirmed again; a Disconnect from another endpoint is refused.

## Rate limiting (server policy, not wire format)

Per endpoint (defaults): 200 packets/s (burst 400) and 256 KiB/s (burst 512 KiB); Hello 2/s (burst 6); globally
1000 new sessions/s (burst 2000). The tracking table holds at most 65 536 endpoints; entries whose bucket is full are
pruned, and new endpoints are refused while the table is full of busy ones. The limiter does not distinguish channels
(it may drop acks); the reliability layer repairs that.
