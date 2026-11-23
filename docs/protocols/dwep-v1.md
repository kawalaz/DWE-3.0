> **Superseded by [DWEP v2](dwep-v2.md) in DWE 0.3.0.** Kept for history. v1 and v2 peers reject each other.

# DWEP v1 — DWE Packet Envelope

All integer fields are big-endian. The 12-byte envelope is unchanged from Foundation 0.1.0.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic: ASCII `DWE1` |
| 4 | 1 | Version: `1` |
| 5 | 1 | Type |
| 6 | 4 | Sequence |
| 10 | 2 | Payload length |
| 12 | N | Payload |

Total datagram size must be 12–1200 bytes. The declared payload length must match the actual datagram size exactly.
Unknown message types and unsupported protocol versions are rejected.

## Message types

| Value | Name | Direction | Payload |
|---:|---|---|---|
| 1 | Ping | client → server | application bytes (legacy test) |
| 2 | Echo | server → client | echoed bytes (legacy test) |
| 3 | Error | server → client | UTF-8 diagnostic text |
| 4 | Hello | client → server | empty |
| 5 | Welcome | server → client | 8-byte session ID |
| 6 | Disconnect | client → server | 8-byte session ID |
| 7 | ChannelData | both | session ID + channel ID + application data |

## ChannelData payload

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Session ID, unsigned big-endian; zero is invalid |
| 8 | 1 | Channel ID |
| 9 | N | Application data |

Channel IDs: Control=0, Input=1, Snapshot=2, Combat=3, Inventory=4, Destruction=5, WorldSync=6.
The channel names are logical routing labels only. At this stage, all data uses raw UDP and therefore has no delivery or ordering guarantee.

## Session rules

- A client sends an empty Hello packet.
- The server allocates a process-local session ID and responds with Welcome.
- The server associates the session with the source IPv4 address and UDP port.
- ChannelData and Disconnect must present the session ID from the same endpoint.
- Duplicate Hello packets from the same endpoint reuse its current session.
- Idle sessions expire after the configured timeout (15 seconds by default).
- The session ID is not an authentication secret and this protocol does not prevent all spoofing or replay attacks.
