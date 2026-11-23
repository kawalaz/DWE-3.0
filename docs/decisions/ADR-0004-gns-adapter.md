# ADR-0004: GameNetworkingSockets adapter strategy

**Status:** Proposed - implemented as **experimental and unverified** in 0.3.0

## Decision
Provide `GnsTransport` as an `ITransport` implementation behind `-DDWE_WITH_GNS=ON` (default OFF, so the default
build has no third-party dependency).

* **DWE reliability stays on top.** DWE messages are sent as *unreliable* GNS messages
  (`k_nSteamNetworkingSend_UnreliableNoNagle`); acknowledgement, retransmission and sequencing are still done by the
  DWE layer. Both transports therefore expose identical semantics, which makes the comparison announced in the 0.2.0
  roadmap fair (same protocol, different transport).
* **Mapping of datagrams to connections:** a GNS connection is identified by its remote `address:port`. A server
  (`open(port != 0)`) listens, accepts every connection and reports each message with the peer's address and port;
  `send_to(address, port)` answers over that connection. A client's first `send_to` opens the connection.
* **Limitations of the mapping:** numeric IP addresses only (no name resolution); no ephemeral server port;
  datagrams sent while a connection is being established may be dropped (the DWE Hello is retransmitted anyway).
* What GNS adds on top (encryption, authentication, its own flow/congestion control, NAT traversal) is **not used yet**.
  Using GNS's own reliability instead of the DWE layer would need a transport capability flag
  (`provides_reliability`) and is a later step.

## Status of the implementation (honest)
The adapter was written against the public GNS API but **never compiled or run against the real library**: the
environment it was written in had neither the headers nor network access. It was only syntax- and type-checked against a
hand-written stub of the API, which proves nothing about the real API. Before trusting it:

1. Build with GNS available: `cmake -S . -B build -DDWE_WITH_GNS=ON` (the CMake target names tried are
   `GameNetworkingSockets::GameNetworkingSockets`, `::shared`, `::GameNetworkingSockets_s`).
2. Run `ctest -R dwe_transport_contract`. It exercises raw datagram exchange, the reported source address, the
   maximum-size datagram, reopening, and a complete DWE session (handshake, one echo per channel, graceful
   disconnect) for every compiled-in transport. Expect to fix API details (callback signature, type names).
3. Compare with UDP: `dwe_server --transport gns` / `dwe_loadgen --mode udp --transport gns`.

## Consequences
The `ITransport` boundary was sufficient (as predicted in ADR-0002) without connection handles, because connections are
keyed by endpoint inside the adapter. A richer interface will be needed once peer lifecycle events (connected / closed
by peer) must reach the session layer.
