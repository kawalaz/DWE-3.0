# DWE Foundation 0.3.0 - Network Reliability

Standalone C++20 networking foundation for the Distributed World Engine.
Unreal Engine 5 remains the planned game client/presentation layer; this repository builds the server-side runtime independently.

## What changed in 0.3.0

UDP delivers neither all packets nor in order. 0.3.0 adds a reliability layer **per logical channel**. In 0.2.0 the
channels were only labels; now each one has a real delivery guarantee:

| Channel | Delivery | Guarantee |
|---|---|---|
| Control, Combat, Inventory, Destruction, WorldSync | `ReliableOrdered` | acknowledged, retransmitted, delivered **exactly once and in order** |
| Input | `Unreliable` | never retransmitted; duplicates and too-old datagrams dropped |
| Snapshot | `UnreliableSequenced` | never retransmitted; only messages newer than the newest delivered are delivered |

| # | Feature | Status |
|---|---|---|
| 1 | ACK (cumulative + selective) | done |
| 2 | Retransmission (RTO + backoff, fast retransmit, bounded window and backlog) | done |
| 3 | Reliable and unreliable channels | done |
| 4 | Sequence tracking (duplicates, stale packets, 32-bit wrap) | done |
| 5 | Rate limiting (per endpoint, Hello limits, bounded tables) | done |
| 6 | Load generator `dwe_loadgen` (hundreds to thousands of clients) | done |
| 7 | GameNetworkingSockets adapter | **experimental - written but NOT compiled/run against the real library** |

Also new: deterministic network simulator, `Server` / `ClientConnection` classes, keep-alive, confirmed disconnect,
client nonce in Hello, DWEP **v2** (v1 peers are rejected). See `CHANGELOG.md` for the full list, including the bugs
fixed relative to 0.2.0.

## Requirements

- CMake 3.20+
- C++20 compiler: Visual Studio 2022 (MSVC) on Windows, GCC 11+ or Clang 14+ on Linux.
- No third-party packages are required (GameNetworkingSockets only for the optional `-DDWE_WITH_GNS=ON`).

## Build on Windows 11 (PowerShell)

```powershell
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

Run the server (UDP port 27015, maximum 128 sessions by default) and, in a second terminal, a client:

```powershell
.\build\Debug\dwe_server.exe 27015 128
.\build\Debug\dwe_client.exe 127.0.0.1 27015 "hello DWE 0.3.0"
```

With a single-config generator (e.g. Ninja), executables are directly under `build/`.

## Build on Linux

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/dwe_server 27015 128
./build/dwe_client 127.0.0.1 27015 "hello DWE 0.3.0"
```

## Programs

```
dwe_server [port] [max-sessions] [--transport udp|gns] [--verbose] [--no-echo]
           [--rate-pps N] [--rate-kbps N] [--no-rate-limit] [--drop P] [--stats-interval SEC]
dwe_client <host> <port> [message] [--transport udp|gns] [--drop P] [--timeout SEC]
dwe_loadgen [--mode sim|udp] [--clients N] [--duration SEC] [--ramp-rate N/s] [--drain SEC] [--seed S]
            [--reliable-hz H] [--input-hz H] [--snapshot-hz H] [--payload B]
            sim: [--loss P] [--dup P] [--reorder P] [--latency MS] [--jitter MS] [--no-server-rate-limit]
            udp: [--host H] [--port P] [--threads N] [--drop P] [--transport udp|gns]
```
`--drop P` makes a fraction `P` of the datagrams vanish on send and on receive (a way to exercise retransmission over
real sockets). Ctrl+C stops the server gracefully: clients are told, and statistics are printed.

### Load testing

Deterministic, in one process (virtual time, reproducible by `--seed`):

```bash
./build/dwe_loadgen --mode sim --clients 5000 --duration 10 --ramp-rate 1000 \
    --loss 0.2 --dup 0.1 --reorder 0.4 --latency 60 --jitter 30
```

Real sockets (start the server first; each client opens its own socket, so the file-descriptor limit must allow it):

```bash
./build/dwe_server 27015 3000 &
./build/dwe_loadgen --mode udp --port 27015 --clients 2000 --duration 15 --threads 2
```

Every client sends numbered messages on every channel and checks the echoes: reliable channels must return each message
exactly once and in order, unreliable ones must never return duplicates (loss is expected and reported). The report gives
connection and handshake statistics, round-trip percentiles, loss per channel, retransmission counters and a
`VERDICT`; the exit code is non-zero when a guarantee is violated.

## Protocol (DWEP v2)

Envelope: magic `DWE1`, version **2**, type, 32-bit sequence, 16-bit payload length, payload (0-1188 bytes);
the complete datagram is at most 1200 bytes; integers are big-endian. Message types: Ping, Echo, Error, Hello, Welcome,
Disconnect, ChannelData, **Ack**, **DisconnectAck**. A message must fit one datagram (<= 1179 bytes of application data).
Full specification: `docs/protocols/dwep-v2.md`. Design and parameters: `docs/architecture/reliability.md`.

## Rate limiting (defaults)

Per endpoint 200 packets/s (burst 400) and 256 KiB/s (burst 512 KiB); `Hello` 2/s (burst 6) per endpoint and 1000 new
sessions/s globally; at most 65 536 tracked endpoints. Packets over a limit are dropped silently. Realistic game traffic
(60 Hz input, 30 Hz snapshots, a few reliable messages) stays well under the limits; a client sending 200 Hz is throttled
while its reliable messages are still delivered. The limiter does not distinguish channels.

## Tests

`ctest` runs 13 tests: packet codec, sequence tracking, CLI parsing, reliable channel (including randomized loss /
duplication / reordering and 32-bit wrap), channel manager, rate limiter, simulated network, UDP socket, connection
manager, end-to-end reliability over the simulator, a real-UDP flow with 25% loss per direction, the transport
contract, and a load-generator smoke run. For a longer soak: `DWE_TEST_SEEDS=300 ./build/dwe_reliability_tests`.

## GameNetworkingSockets (experimental)

`-DDWE_WITH_GNS=ON` compiles `GnsTransport`. It was written against the public API **without access to the library**
and has **never been compiled against it or run**. After building with GNS installed, run
`ctest -R dwe_transport_contract` - it checks the adapter end to end - and expect to fix API details. See
`docs/decisions/ADR-0004-gns-adapter.md`.

## Important limitations

- **No fragmentation:** a message above 1179 bytes is refused (`TooLarge`).
- **No congestion control:** the window (64 messages per channel) and the rate limiter bound the load; senders do not slow down.
- **No authentication, encryption or anti-spoofing handshake.** A spoofed-source Hello flood can still fill the session table
  (within the global Hello limit); session IDs are small sequential numbers, not secrets; reflection by spoofed sources is only bounded by the rate limiter.
- The connection survives up to roughly 25% loss per direction; reliable data is repaired beyond that, but the keep-alive
  eventually declares the server dead (see `docs/architecture/reliability.md`).
- A reliable channel that makes no acknowledgement progress for 10 retransmissions closes the whole session.
- `disconnect()` does not flush unacknowledged reliable messages.
- The server is a single-threaded receive loop; replication, region ownership and persistence are future milestones.
- `config/server/development.json` documents the defaults but is not loaded; use the command-line flags.
- The echo server is a test handler, not gameplay.

Do not expose this development server to the public internet as a production service.

## Next milestone

Handshake hardening (stateless cookies, authentication hooks, replay protection), fragmentation and congestion control,
verifying and benchmarking the GNS adapter against UDP, then replication snapshots and interest management.
