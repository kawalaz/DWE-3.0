#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <cstddef>

namespace dwe::network {

struct Datagram {
    std::vector<std::uint8_t> bytes;
    std::string address;
    std::uint16_t port{0};
};

class UdpSocket {
public:
    UdpSocket();
    ~UdpSocket();
    UdpSocket(const UdpSocket&) = delete;
    UdpSocket& operator=(const UdpSocket&) = delete;
    UdpSocket(UdpSocket&& other) noexcept;
    UdpSocket& operator=(UdpSocket&& other) noexcept;

    bool open(std::uint16_t port, std::string* error = nullptr);
    bool send_to(const std::string& host, std::uint16_t port,
                 const std::vector<std::uint8_t>& bytes, std::string* error = nullptr);

    // Waits up to timeout_ms for one datagram.
    // Returns true only for a datagram of at most dwe::core::kMaxDatagramSize bytes.
    // Returns false on timeout (error is left empty) or on failure (error is set).
    // Oversized datagrams are consumed and rejected, never truncated.
    bool receive(Datagram& datagram, int timeout_ms, std::string* error = nullptr);

    // Local port the socket is bound to (useful after open(0)); 0 if not open.
    std::uint16_t local_port() const;

    void close();

    // Requests OS socket buffer sizes (best effort; the OS may cap them).
    void set_buffer_sizes(std::size_t receive_bytes, std::size_t send_bytes);
    // Size the OS actually granted (Linux reports twice the requested value); 0 if not open.
    int receive_buffer_bytes() const;

private:
#ifdef _WIN32
    std::uintptr_t socket_{static_cast<std::uintptr_t>(~0ULL)};
#else
    int socket_{-1};
#endif
    bool initialized_{false};
};

} // namespace dwe::network
