#pragma once

#include "dwe/network/ITransport.h"

#include <memory>
#include <cstdint>
#include <string>
#include <vector>

namespace dwe::network {

// EXPERIMENTAL - compiled only with -DDWE_WITH_GNS=ON.
//
// ITransport adapter on top of Valve's GameNetworkingSockets (GNS). It was written against the
// public GNS API (ISteamNetworkingSockets) but has NOT been compiled or run against the real
// library yet; run the transport contract test (tests/integration/TransportContractTests.cpp,
// ctest name dwe_transport_contract) on a machine that has GNS before relying on it.
//
// How the datagram-style ITransport maps onto GNS's connection-oriented API:
//  * open(port != 0)  -> server: creates a listen socket and a poll group and accepts every incoming
//                        connection. open(0) -> client: no listen socket.
//  * Every GNS connection is identified by its remote "address:port"; that pair is what receive()
//    reports and what send_to() takes, so ConnectionManager keeps working unchanged.
//  * A client's first send_to(host, port) opens the connection. host must be a numeric IP address.
//  * Messages are sent UNRELIABLE (k_nSteamNetworkingSend_UnreliableNoNagle): the DWE reliability
//    layer (ACK / retransmission / sequencing) runs on top exactly as it does over UDP, so both
//    transports can be compared like for like. Datagrams sent before the connection is established
//    may be dropped; DWE's Hello retransmission covers that.
//  * An ephemeral server port (open(0) on the server side) is not supported.
//  * Not thread-safe; use one instance per thread.
class GnsTransport final : public ITransport {
public:
    GnsTransport();
    ~GnsTransport() override;
    GnsTransport(const GnsTransport&) = delete;
    GnsTransport& operator=(const GnsTransport&) = delete;

    bool open(std::uint16_t local_port, std::string* error = nullptr) override;
    bool send_to(const std::string& host, std::uint16_t port, const std::vector<std::uint8_t>& bytes,
                 std::string* error = nullptr) override;
    bool receive(TransportDatagram& datagram, int timeout_ms, std::string* error = nullptr) override;
    std::uint16_t local_port() const override;
    void close() override;

    struct Impl;  // opaque: keeps the GNS headers out of every translation unit that includes this one

private:
    std::unique_ptr<Impl> impl_;
};

} // namespace dwe::network
