// Loopback tests for dwe::network::UdpSocket. Uses ephemeral ports on 127.0.0.1 only.
#include "dwe/core/Packet.h"
#include "dwe/network/UdpSocket.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#endif

namespace {
int failures = 0;
void check(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

// Deliberately bypasses UdpSocket::send_to() validation to emulate an untrusted external peer.
bool send_raw_datagram(const char* host, std::uint16_t port, const std::vector<std::uint8_t>& bytes) {
#ifdef _WIN32
    const SOCKET raw = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (raw == INVALID_SOCKET) return false;
#else
    const int raw = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (raw < 0) return false;
#endif
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &destination.sin_addr) != 1) {
#ifdef _WIN32
        closesocket(raw);
#else
        ::close(raw);
#endif
        return false;
    }
#ifdef _WIN32
    const int sent = sendto(raw, reinterpret_cast<const char*>(bytes.data()),
                            static_cast<int>(bytes.size()), 0,
                            reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
    closesocket(raw);
    return sent >= 0 && static_cast<std::size_t>(sent) == bytes.size();
#else
    const auto sent = sendto(raw, bytes.data(), bytes.size(), 0,
                             reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
    ::close(raw);
    return sent >= 0 && static_cast<std::size_t>(sent) == bytes.size();
#endif
}
}

int main() {
    using dwe::core::kMaxDatagramSize;
    using dwe::network::Datagram;
    using dwe::network::UdpSocket;

    UdpSocket receiver;
    UdpSocket sender;
    std::string error;
    check(receiver.open(0, &error), "open receiver on ephemeral port");
    check(sender.open(0, &error), "open sender on ephemeral port");
    const std::uint16_t port = receiver.local_port();
    check(port != 0, "receiver reports its bound port");

    Datagram datagram;

    // A timeout is "false with no error", which is how callers tell it from a failure.
    check(!receiver.receive(datagram, 50, &error), "receive times out when nothing was sent");
    check(error.empty(), "timeout leaves error empty");

    // Plain round trip.
    const std::vector<std::uint8_t> hello = {1, 2, 3};
    check(sender.send_to("127.0.0.1", port, hello, &error), "send small datagram");
    check(receiver.receive(datagram, 1000, &error), "receive small datagram");
    check(datagram.bytes == hello, "small datagram content intact");
    check(datagram.address == "127.0.0.1", "sender address reported");
    check(datagram.port == sender.local_port(), "sender port reported");

    // Exactly the maximum size is accepted.
    const std::vector<std::uint8_t> max_size(kMaxDatagramSize, 0x5A);
    check(sender.send_to("127.0.0.1", port, max_size, &error), "send datagram of kMaxDatagramSize");
    check(receiver.receive(datagram, 1000, &error), "accept datagram of exactly kMaxDatagramSize");
    check(datagram.bytes == max_size, "max-size datagram content intact");

    // Regression: oversized datagrams used to be silently truncated to kMaxDatagramSize and then
    // looked valid. They must be rejected outright (never delivered, even in truncated form).
    for (const std::size_t size : {kMaxDatagramSize + 1, std::size_t{1300}, std::size_t{1500}, std::size_t{4000}}) {
        const std::vector<std::uint8_t> oversized(size, 0x77);
        check(!sender.send_to("127.0.0.1", port, oversized, &error),
              "DWE sender rejects oversized datagram before network send");
        check(!error.empty(), "oversized send reports an error");
        check(send_raw_datagram("127.0.0.1", port, oversized),
              "raw system socket sends oversized datagram for receive-side test");
        datagram.bytes.clear();
        check(!receiver.receive(datagram, 1000, &error), "reject oversized datagram");
        check(!error.empty(), "oversized datagram reports an error");
        check(datagram.bytes.empty(), "oversized datagram is not delivered, not even truncated");

        // The socket must stay usable and the oversized datagram must not linger in the queue.
        check(sender.send_to("127.0.0.1", port, hello, &error), "send after oversized datagram");
        check(receiver.receive(datagram, 1000, &error), "receive valid datagram after oversized one");
        check(datagram.bytes == hello, "valid datagram after oversized one is intact");
    }

    // Socket buffer tuning really reaches the OS: a tiny request shrinks the receive buffer,
    // a large one grows it again (up to whatever the OS allows).
    {
        UdpSocket tuned;
        check(tuned.open(0, &error), "open socket for buffer tuning");
        const int before = tuned.receive_buffer_bytes();
        check(before > 0, "receive buffer size is readable");
        tuned.set_buffer_sizes(4096, 4096);
        const int small = tuned.receive_buffer_bytes();
        check(small > 0 && small < before, "a small request shrinks the receive buffer");
        tuned.set_buffer_sizes(4u << 20, 1u << 20);
        check(tuned.receive_buffer_bytes() > small, "a large request grows it again");
    }

    // Closed sockets fail cleanly.
    receiver.close();
    check(receiver.local_port() == 0, "closed socket has no local port");
    check(!receiver.receive(datagram, 10, &error) && !error.empty(), "receive on closed socket fails");
    check(!receiver.send_to("127.0.0.1", port, hello, &error), "send on closed socket fails");

    if (failures == 0) std::cout << "All DWE UDP socket tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
