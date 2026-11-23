# ADR-0002: Transport abstraction

**Status:** Accepted

DWE networking code uses `ITransport` instead of depending directly on UDP socket APIs.

**Decision:** `UdpTransport` is the first implementation. Other transports, including a future GameNetworkingSockets adapter, can be added behind the interface without changing DWEP packet parsing or session lifecycle logic.

**Trade-off:** The current interface is intentionally small and datagram-oriented. A connection-oriented transport may require a later interface extension for peer lifecycle events and connection handles.
