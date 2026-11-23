#include "dwe/network/UdpTransport.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dwe::network {

bool UdpTransport::open(std::uint16_t local_port, std::string* error) {
    return socket_.open(local_port, error);
}

bool UdpTransport::send_to(const std::string& host, std::uint16_t port,
                           const std::vector<std::uint8_t>& bytes, std::string* error) {
    return socket_.send_to(host, port, bytes, error);
}

bool UdpTransport::receive(TransportDatagram& datagram, int timeout_ms, std::string* error) {
    Datagram udp_datagram;
    if (!socket_.receive(udp_datagram, timeout_ms, error)) return false;
    datagram.bytes = std::move(udp_datagram.bytes);
    datagram.address = std::move(udp_datagram.address);
    datagram.port = udp_datagram.port;
    return true;
}

std::uint16_t UdpTransport::local_port() const { return socket_.local_port(); }
void UdpTransport::close() { socket_.close(); }

void UdpTransport::tune_buffers(std::size_t receive_bytes, std::size_t send_bytes) {
    socket_.set_buffer_sizes(receive_bytes, send_bytes);
}

} // namespace dwe::network
