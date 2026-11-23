#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dwe::network {

struct TransportDatagram {
    std::vector<std::uint8_t> bytes;
    std::string address;
    std::uint16_t port{0};
};

// Transport boundary: the DWE protocol and session layer must not depend on UDP/Winsock APIs.
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual bool open(std::uint16_t local_port, std::string* error = nullptr) = 0;
    virtual bool send_to(const std::string& host, std::uint16_t port,
                         const std::vector<std::uint8_t>& bytes,
                         std::string* error = nullptr) = 0;
    // false + empty error means timeout; false + non-empty error means transport failure/drop.
    virtual bool receive(TransportDatagram& datagram, int timeout_ms,
                         std::string* error = nullptr) = 0;
    virtual std::uint16_t local_port() const = 0;
    virtual void close() = 0;
    // Optional hint, called after open(): ask the OS for larger socket buffers. A server funnels every
    // client through one socket, so a short scheduling stall must not overflow the receive queue.
    // Best effort (the OS caps it); transports without such buffers ignore it.
    virtual void tune_buffers(std::size_t /*receive_bytes*/, std::size_t /*send_bytes*/) {}
};

} // namespace dwe::network
