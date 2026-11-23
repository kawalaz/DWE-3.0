# DWE reliability layer (0.3.0)

UDP gives no delivery, ordering or duplicate protection. DWE 0.3.0 adds them **per logical channel**, in a layer
that is independent of the socket and of the clock.

## Layers

```
 application (game / echo handler)
     |  send(session, channel, bytes)            ^ delivered messages (in order / newest / deduplicated)
 +---v------------------------------------------+---+
 | Server / ClientConnection   handshake, keep-alive, disconnect, I/O loop                  |
 | ConnectionManager           sessions, endpoint binding, rate limits, timers              |
 | ChannelManager (per session) 7 channels, ack packing, RTT estimator                      |
 | ReliableChannel (x5)  window, retransmission, reorder buffer   | Sequence.h  NewestSequence / SequenceWindow |
 | Packet codec (dwe_core)     DWEP v2 envelope, Ack / Disconnect / Hello / channel payloads |
 +-----------------------------------------------+---+
     |  ITransport:  UdpTransport | SimTransport | LossyTransport | GnsTransport (experimental)
```
`ReliableChannel`, `ChannelManager` and `ConnectionManager` are pure state machines: they take `now` as an argument and
never read a clock or touch a socket, so the whole stack runs under a virtual clock in `SimulatedNetwork`.
Threading model: everything is single-threaded; `dwe_loadgen --mode udp --threads N` runs independent client groups.

## Parameters and why

| Parameter | Default | Reason |
|---|---:|---|
| window | 64 messages | the Ack bitmap is 64 bits; ~1280 msg/s per channel at 50 ms RTT |
| send backlog | 256 messages | bounded memory; `QueueFull` is back-pressure, never silent loss |
| RTO | 250 ms initial, 100 ms..2 s | RFC 6298 (`SRTT + max(1 ms, 4 RTTVAR)`), doubled per retry |
| fast retransmit | 3 informative acks | repairs a gap in about one RTT instead of one RTO |
| retransmit budget | 10 without progress | ~13 s of total silence; any new ack resets it |
| reorder buffer | 64 slots, 32 KiB | bounds memory a (malicious) peer can pin per channel |
| keep-alive | every 0.5 s when idle | see the loss tolerance below |
| server timeout (client) | 10 s | |
| session idle timeout (server) | 15 s | |

## Loss tolerance (what is designed for, and what is not)

A keep-alive probe succeeds with probability `(1-p)^2` for loss `p` per direction; the client gives up after
`server_timeout / keepalive_interval = 20` consecutive failures:

| loss per direction | false "server timeout" per 10 s window | chance of at least one per hour |
|---:|---:|---:|
| 10% | 4e-15 | ~0 |
| 20% | 1.3e-9 | ~0 |
| 25% | 6.6e-8 | ~0 |
| 40% | 1.3e-4 | ~5% |
| 50% | 3.2e-3 | ~68% |

DWE is meant to stay connected up to roughly **25% loss per direction**. Reliable channels themselves repair far
worse (the 5000-client run used 20% with 40% reordering), but at 40% a long-lived connection is dropped by the
keep-alive check a few percent of the time per hour and at 50% within minutes. (With the previous 3 s keep-alive
there were only 3 probes per window: 4.7% per window at 20% loss, which is what the load test exposed.)
Unreliable channels are **not** repaired - that is their purpose.

## Measured behaviour

All runs verify, per client, that reliable echoes return exactly once and in order. Measured on a 1-core sandbox
(server and load generator shared the core), so absolute throughput is conservative.

| scenario | result |
|---|---|
| simulated, 5000 clients, 20% loss each way, 10% duplication, 40% reordering, 60 +/- 30 ms | 593 345 reliable messages: 0 lost, 0 duplicated, 0 reordered; 0 connection failures; server ~40% of one core of simulated time (no syscall cost) |
| simulated, 1000 clients, 5% loss, 2% dup, 10% reorder | 206 265 reliable messages: all delivered; server ~9% of one core |
| real UDP loopback, 2000 clients, no artificial loss | ~1 M datagrams at the server, 0 lost, 0 retransmissions |
| real UDP loopback, 1000 clients, ~25% loss per direction (simulated at both ends) | 153 805 reliable messages: all delivered exactly once, in order |
| memory | ~26 KB per client+session pair (5000 sessions + 5000 clients in one process: ~129 MiB) |

Reproduce: `dwe_loadgen --mode sim --clients 5000 --duration 10 --loss 0.2 --dup 0.1 --reorder 0.4 --latency 60 --jitter 30`,
or start `dwe_server` and run `dwe_loadgen --mode udp --clients 2000`.

## Failure semantics

* A reliable channel that makes no acknowledgement progress for 10 retransmissions fails; the session is closed
  (the peer is told with Disconnect(Timeout) best effort) because an ordered stream cannot skip a message.
* `disconnect()` does not flush: reliable messages not yet acknowledged are lost. Wait for `reliable_idle()` first.
* A restarted client (new nonce) gets a fresh session; the old one is dropped.

## What this layer does not do (yet)

* **Fragmentation:** messages above 1179 bytes are refused (`TooLarge`).
* **Congestion control:** the window (64 messages/channel) and the rate limiter bound the load, but the sender does not
  slow down when the network is congested.
* **Reliable-unordered** delivery; **ordering across channels**.
* **Authentication, encryption, anti-spoofing cookies.** A spoofed-source Hello flood can still fill the session table
  within the global Hello limit; session IDs are small sequential numbers, not secrets (the endpoint check and, on the
  client, source pinning are what protect a session).
* **Priority-aware rate limiting:** the limiter drops by endpoint, not by channel.
* The retransmission and idle timers of the server are scanned every 5 ms (O(sessions) per scan); a timer wheel would
  scale better beyond tens of thousands of sessions.

## Testing approach

* Unit tests per component, plus **randomized** tests (loss, duplication, reordering, 32-bit wrap) that must hold for
  every seed; `DWE_TEST_SEEDS=300 dwe_reliability_tests` is the soak.
* The whole system (server + clients + network) runs deterministically under a virtual clock; the same seed reproduces
  the same run bit for bit.
* Test effectiveness was checked by **mutation**: ~35 deliberate defects (no re-ack of duplicates, ignored nonce, missing
  endpoint check, broken fast retransmit, ...) were injected; the suite detects all of them (the survivors found along the way pointed at real coverage gaps, which were closed).
* Sanitizers: ASan + UBSan on the whole suite, TSan on the multi-threaded load generator.
