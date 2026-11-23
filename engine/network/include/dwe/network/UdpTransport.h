#pragma once

#include "dwe/network/ITransport.h"
#include "dwe/network/UdpSocket.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dwe::network {

class UdpTransport final : public ITransport {
public:
    bool open(std::uint16_t local_port, std::string* error = nullptr) override;
    bool send_to(const std::string& host, std::uint16_t port,
                 const std::vector<std::uint8_t>& bytes,
                 std::string* error = nullptr) override;
    bool receive(TransportDatagram& datagram, int timeout_ms,
                 std::string* error = nullptr) override;
    std::uint16_t local_port() const override;
    void close() override;
    void tune_buffers(std::size_t receive_bytes, std::size_t send_bytes) override;
    int receive_buffer_bytes() const { return socket_.receive_buffer_bytes(); }

private:
    UdpSocket socket_;
};

} // namespace dwe::network
