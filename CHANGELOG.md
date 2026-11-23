# Changelog

## 0.3.0 - Network Reliability

**Breaking:** the wire protocol is now DWEP **v2**. v1 and v2 peers reject each other's packets
(`unsupported protocol version`); there is no negotiation. `ConnectionManager` no longer echoes
application data itself (see "Changed").

### Added
- **ACK + retransmission.** `Ack` message (selective: cumulative `next_expected` + 64-bit bitmap),
  RTT estimation (RFC 6298), exponential backoff, fast retransmit, bounded send window (64) and
  bounded backlog with back-pressure (`SendStatus::QueueFull`).
- **Reliable and unreliable channels with different delivery mechanisms** (fixed by the protocol):
  `ReliableOrdered` (Control, Combat, Inventory, Destruction, WorldSync), `Unreliable` with a
  duplicate / too-old window (Input), `UnreliableSequenced` - newest wins (Snapshot).
- **Sequence tracking.** Per-channel 32-bit sequence numbers with wrap-safe comparison
  (`dwe/core/Sequence.h`), duplicate and stale-packet detection on every channel.
- **Rate limiting.** Token buckets per endpoint (packets/s and bytes/s), a much tighter Hello limit
  per endpoint, a global session-creation limit, bounded limiter tables (spoofed-source floods cannot
  exhaust memory). Rate-limited datagrams get no reply.
- **Load generator** `dwe_loadgen`: `--mode sim` (deterministic, virtual time, in-process server,
  loss/duplication/reordering/latency model) and `--mode udp` (real sockets, multi-threaded, optional
  simulated loss). It verifies exactly-once / in-order delivery and exits non-zero on any violation.
- **Network impairment simulator** `SimulatedNetwork` (seeded, reproducible) and `LossyTransport`.
- **GameNetworkingSockets adapter** `GnsTransport` - **experimental, not compiled or run against the
  real library** (`-DDWE_WITH_GNS=ON`, off by default) - plus a transport contract test to verify it.
- `Server` and `ClientConnection` classes (the runtime moved out of `main()`): handshake with
  retransmission, keep-alive (Ping/Echo with RTT), graceful Disconnect with confirmation, server-initiated
  Disconnect on shutdown/timeout. Both take an injected transport and clock.
- `DisconnectAck` message; Disconnect payload now carries a reason.
- Socket buffer tuning (`ITransport::tune_buffers`); the server asks for 4 MiB receive / 1 MiB send.
- Server/client/loadgen options: `--transport`, `--drop`, rate-limit flags, `--stats-interval`.
- New tests: sequence, reliable channel (incl. randomized loss/dup/reorder, wrap-around), channel
  manager, rate limiter, simulated network, connection manager (ported + extended), end-to-end
  reliability over the simulator, real-UDP flow with loss, transport contract, load-generator smoke.

### Changed
- `Hello` must carry an 8-byte client nonce. A repeated Hello with the same nonce is a retransmission
  (answered idempotently); a different nonce from the same endpoint is a restarted client and replaces
  the old session, so stale sequence state is never inherited.
- `Ping`/`Echo` are now a session-scoped keep-alive and RTT probe.
- `ConnectionManager::process` returns delivered application messages (`result.delivered`) instead of
  echoing them; the echo is now a handler of `Server`.
- Per-endpoint limiter and `admit()` run before packet parsing.

### Fixed (relative to 0.2.0)
- `process()` swept the whole session table for expired sessions on **every packet** (O(sessions) per
  packet). Expiry is now exact per access plus a periodic sweep.
- Any unsupported message type, including `Error`, was answered with an `Error`: two servers could bounce
  packets forever. Reply-class messages (`Echo`, `Error`, `Welcome`, `Ack`, `DisconnectAck`) are never answered.
- The client accepted datagrams from any source. It now pins the server's address after the Welcome.
- Server and client statistics lost the counters of sessions that had already ended.
- Real-UDP load exposed kernel receive-queue overflow (`RcvbufErrors`) with 500 clients; fixed by buffer tuning.
- Keep-alive tuning: probing every 3 s with a 10 s silence limit left only 3 probes per window and disconnected
  healthy clients on lossy links (4.7% of 10 s windows at 20% loss per direction, found by the load test);
  now every 0.5 s = 20 probes (6.6e-8 at 25% loss per direction).
- The retransmission budget was counted per message; a lossy but alive link would eventually exhaust it.
  It now counts retransmissions without acknowledgement progress (as TCP does).

### Known limitations
See README "Important limitations". Notably: no fragmentation (messages must fit one datagram), no
congestion control, no authentication/encryption/handshake cookies, GNS adapter unverified.

## 0.2.0

- Added `ITransport` abstraction and `UdpTransport` adapter.
- Added DWEP message types Hello, Welcome, Disconnect, and ChannelData.
- Added session ID and logical channel payload codecs.
- Added `ConnectionManager` with endpoint-bound sessions, capacity limit, and idle expiry.
- Updated client/server to exercise session handshake and channel round trip.
- Added connection manager and UDP integration tests.
- Enforced 1200-byte maximum on outgoing UDP datagrams.

## 0.1.0

- Initial C++20/CMake foundation.
- DWEP v1 packet envelope, UDP socket, ping/echo sample, and basic tests.
