# DWE Networking - Network Reliability 0.3.0

## Vertical slice

1. DWEP v2 packets are serialized and validated (`dwe_core`), see `docs/protocols/dwep-v2.md`.
2. `ITransport` carries datagrams: `UdpTransport` (default), `SimTransport` (deterministic simulator),
   `LossyTransport` (decorator), `GnsTransport` (experimental).
3. `Server` owns a transport and a `ConnectionManager`; `ClientConnection` is the matching client.
4. Per datagram: **rate limit -> parse -> session/endpoint validation -> channel handling -> application**.
5. Each session owns a `ChannelManager`: three delivery mechanisms over seven channels, with acks, retransmission,
   sequence tracking, RTT estimation (see `reliability.md`).
6. Timers (`update`) flush acks, send queued messages, retransmit, fail dead sessions and expire idle ones.

## Boundaries

* `ITransport` is the seam to the network. It is datagram-oriented; connection-oriented transports map connections to
  endpoints internally (ADR-0004). `tune_buffers()` is an optional hint (server receive queue size).
* `ConnectionManager` owns session lifecycle, rate limits and the per-session channel state. It does not do I/O and does
  not read a clock: `now` is always a parameter. Application data comes out of `process()` as `result.delivered` and
  goes in through `send()`; what the application does with it (the demo server echoes it) lives in `Server`.
* The protocol state machines are deterministic, which is what makes the simulator, the mutation checks and the
  seeded soak tests possible.

## Rules (carried forward)

* Treat every datagram as untrusted input; validate envelope, type, length, channel, session and endpoint first.
* Keep complete datagrams at or below 1200 bytes; never truncate.
* Everything is bounded: queues, windows, buffers, tracked endpoints, sessions.
* Never answer reply-class messages or rate-limited datagrams.
* Keep the authoritative server independent from Unreal Engine.

## Important current limitations

* No fragmentation, no congestion control, no authentication, encryption or anti-spoofing handshake.
* Session IDs are identifiers, not credentials.
* Single-threaded receive loop; the timer scan is O(sessions) every 5 ms.
* The GNS adapter is unverified.

## Planned next steps

1. Handshake hardening (stateless cookies), authentication hooks, replay protection.
2. Fragmentation of large reliable messages; congestion control.
3. Verify and benchmark the GNS adapter against UDP; optionally use GNS's own reliability and encryption.
4. Timer wheel and multi-threaded receive for tens of thousands of sessions.
5. Replication snapshots and spatial interest management.
