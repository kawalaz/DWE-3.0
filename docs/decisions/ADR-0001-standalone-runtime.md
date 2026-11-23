# ADR-0001: Standalone C++ runtime

**Status:** Accepted

DWE server runtime is a standalone C++20 application. Unreal Engine 5 is the client/presentation layer and does not own authoritative world state.

**Reason:** The server must be independently deployable, testable, and scalable without requiring UE runtime on every server process.
